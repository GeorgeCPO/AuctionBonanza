import type { Logger } from "pino";
import { createCluster } from "redis";

import { slotOf } from "./hash-slots.js";
import { readSlotMap } from "./read-slot-map.js";
import type { SlotMap } from "./slot-map.js";
import type {
  BusSubscriber,
  ShardSubscriberClient,
  ValkeyBusSubscriberOptions,
} from "./bus.types.js";
import { SerialByKey } from "../shared/serial-by-key.js";

// Subscribes with SSUBSCRIBE through node-redis's cluster client, which keeps
// one subscriber connection per primary and routes each channel to the
// primary owning its hash slot.
//
// Interruptions are observed on those subscriber connections directly. When
// one reconnects, node-redis sends SSUBSCRIBE again for each of its channels
// and waits for every confirmation before the connection emits "ready", so
// "ready" marks the moment its subscriptions are live again.
//
// When a slot moves to another primary, or a failover replaces one,
// node-redis moves the channels to the new owner internally and reports
// nothing on success; messages published in between are lost silently. So
// the subscriber remembers which primary owned each watched channel when it
// was subscribed and compares that with CLUSTER SHARDS every
// `topologyCheckIntervalMs`. A channel whose owner changed is reported as
// lost, and the registry subscribes it again and resyncs it. The loss is
// bounded by one interval instead of waiting for the auction's next event.
// "Edge shard subscriber" in docs/REALTIME_GATEWAY.md explains why this
// stays on node-redis's cluster client instead of owning the connections.
export class ValkeyBusSubscriber implements BusSubscriber {
  private readonly cluster: ReturnType<typeof createCluster>;
  private readonly logger: Logger;
  private readonly observedClients = new WeakSet<ShardSubscriberClient>();
  // Subscriber connections that were lost and have not resubscribed yet.
  private readonly reconnecting = new Set<ShardSubscriberClient>();
  private readonly interruptionListeners: (() => void)[] = [];
  private readonly subscriptionLostListeners: ((channel: string) => void)[] = [];
  private restoredWaiters: (() => void)[] = [];
  // Subscribe and unsubscribe of one channel run one after another: node-redis
  // would otherwise attach a new listener to a channel whose SUNSUBSCRIBE is
  // still in flight, and drop it when the reply arrives.
  private readonly operationsByChannel = new SerialByKey();
  // The primary that owned each watched channel's slot when it was
  // subscribed, per the slot map of that moment (undefined if none was known).
  private readonly ownersAtSubscribe = new Map<string, string | undefined>();
  private slotMap: SlotMap | undefined;
  private topologyTimer: NodeJS.Timeout | undefined;
  private checkingTopology = false;
  private connected = false;

  constructor(private readonly options: ValkeyBusSubscriberOptions) {
    this.logger = options.logger.child({ component: "valkey" });
    this.cluster = createCluster({
      rootNodes: options.nodes.map((node) => ({ url: `redis://${node}` })),
      defaults: {
        name: options.clientName,
        socket: {
          connectTimeout: options.connectTimeoutMs,
          // Reconnect quickly and indefinitely. Watched auctions stay
          // "syncing" until the connection is back.
          reconnectStrategy: (retries: number) => Math.min(50 * 2 ** retries, 1_000),
        },
      },
    });
    // Without listeners, node-redis re-emits connection errors as unhandled
    // 'error' events, which would crash the process.
    this.cluster.on("error", (error: unknown) =>
      this.logger.error({ err: error }, "Valkey cluster error"),
    );
    this.cluster.on("node-error", (error: unknown, node: unknown) =>
      this.logger.warn({ err: error, node }, "Valkey node error"),
    );
    // node-redis removes a moved channel's listener before following the
    // move. If following fails, nothing listens on the channel any more, so
    // no later event could reveal what is missed: the edge must subscribe
    // again. (A successful move is not reported at all; see the class
    // comment.)
    this.cluster.on("sharded-channel-moved-error", (error: unknown, channel: unknown) => {
      this.logger.error({ err: error, channel }, "could not follow a moved sharded channel");
      if (typeof channel !== "string") {
        return;
      }
      for (const listener of this.subscriptionLostListeners) {
        listener(channel);
      }
    });
    // Emitted once for every new connection, including the subscriber
    // connections node-redis opens on its own after a topology change.
    this.cluster.on("node-connect", () => this.observeShardSubscribers());
  }

  async connect(): Promise<void> {
    await this.cluster.connect();
    this.connected = true;
    this.slotMap = await this.readSlotMapOrUndefined();
    this.topologyTimer = setInterval(
      () => void this.checkTopology(),
      this.options.topologyCheckIntervalMs,
    );
    this.topologyTimer.unref();
  }

  subscribe(channel: string, listener: (message: string) => void): Promise<void> {
    return this.operationsByChannel.run(channel, async () => {
      if (this.ownersAtSubscribe.has(channel)) {
        // Subscribed before and reported lost: drop what node-redis still
        // holds, so the new listener is not added next to the old one.
        await this.cluster.sUnsubscribe(channel).catch(() => undefined);
      }
      // Read before subscribing: a move during the subscribe then shows up as
      // a changed owner at the next check.
      const owner = this.slotMap?.ownerOf(slotOf(channel));
      await this.cluster.sSubscribe(channel, listener);
      this.ownersAtSubscribe.set(channel, owner);
    });
  }

  unsubscribe(channel: string): Promise<void> {
    return this.operationsByChannel.run(channel, async () => {
      await this.cluster.sUnsubscribe(channel);
      this.ownersAtSubscribe.delete(channel);
    });
  }

  onInterruption(listener: () => void): void {
    this.interruptionListeners.push(listener);
  }

  onSubscriptionLost(listener: (channel: string) => void): void {
    this.subscriptionLostListeners.push(listener);
  }

  whenRestored(): Promise<void> {
    if (this.reconnecting.size === 0) {
      return Promise.resolve();
    }
    return new Promise((resolve) => this.restoredWaiters.push(resolve));
  }

  async close(): Promise<void> {
    if (!this.connected) {
      return;
    }
    this.connected = false;
    clearInterval(this.topologyTimer);
    await this.cluster.close();
  }

  // Reports the watched channels whose slot has another owner than the one
  // they were subscribed at: node-redis followed the move silently, so
  // messages published meanwhile may have been lost.
  private async checkTopology(): Promise<void> {
    if (this.checkingTopology || !this.connected) {
      return;
    }
    this.checkingTopology = true;
    try {
      const slotMap = await this.readSlotMapOrUndefined();
      if (slotMap === undefined || !this.connected) {
        return;
      }
      this.slotMap = slotMap;
      const moved: string[] = [];
      for (const [channel, subscribedAt] of this.ownersAtSubscribe) {
        const owner = slotMap.ownerOf(slotOf(channel));
        if (subscribedAt !== undefined && owner !== undefined && owner !== subscribedAt) {
          moved.push(channel);
        }
        if (owner !== undefined) {
          this.ownersAtSubscribe.set(channel, owner);
        }
      }
      if (moved.length > 0) {
        this.logger.warn({ channels: moved.length }, "slots moved; watched auctions will resync");
      }
      for (const channel of moved) {
        for (const listener of this.subscriptionLostListeners) {
          listener(channel);
        }
      }
    } finally {
      this.checkingTopology = false;
    }
  }

  private async readSlotMapOrUndefined(): Promise<SlotMap | undefined> {
    try {
      return await readSlotMap(this.options.nodes, this.options.connectTimeoutMs);
    } catch (error) {
      this.logger.warn({ err: error }, "could not read the cluster topology");
      return undefined;
    }
  }

  // Attaches to subscriber connections not seen before. `masters` and their
  // `pubSub` clients are part of node-redis's typed cluster API.
  private observeShardSubscribers(): void {
    for (const master of this.cluster.masters) {
      const client = master.pubSub?.client;
      if (client === undefined || this.observedClients.has(client)) {
        continue;
      }
      this.observedClients.add(client);
      const node = { host: master.host, port: master.port };

      client.on("reconnecting", () => {
        if (this.reconnecting.has(client)) {
          return;
        }
        this.reconnecting.add(client);
        this.logger.warn(
          { node },
          "Valkey subscriber connection lost; watched auctions will resync",
        );
        for (const listener of this.interruptionListeners) {
          listener();
        }
      });
      client.on("ready", () => {
        const wasReconnecting = this.reconnecting.delete(client);
        if (!wasReconnecting) {
          return;
        }
        this.logger.info({ node }, "Valkey subscriber connection restored and resubscribed");
        this.releaseRestoredWaiters();
      });
      // A connection closed for good (on unsubscribing its last channel, on
      // shutdown, or when a topology refresh dropped its primary) will not
      // come back. In the last case node-redis is already resubscribing its
      // channels at their new owner, fire and forget, and nothing reports
      // when that is confirmed: a resync released here may read before it
      // is, and miss an event published in between. Closing that gap needs
      // the shard subscriber (step 2); see "Edge implementation status".
      client.on("end", () => {
        const wasReconnecting = this.reconnecting.delete(client);
        if (wasReconnecting) {
          this.releaseRestoredWaiters();
        }
      });
    }
  }

  private releaseRestoredWaiters(): void {
    if (this.reconnecting.size > 0) {
      return;
    }
    const waiters = this.restoredWaiters;
    this.restoredWaiters = [];
    for (const resolve of waiters) {
      resolve();
    }
  }
}
