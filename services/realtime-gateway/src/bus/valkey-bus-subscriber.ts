import type { Logger } from "pino";
import { createCluster } from "redis";

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
// Known gap: when a slot moves to another primary, or a failover replaces
// one, node-redis moves the channels to the new owner internally and reports
// nothing on success. Messages published in between are lost without an
// interruption, and the edge only notices at the next event of an affected
// auction. A shard subscriber of our own replaces this class in a later
// step; see "Edge shard subscriber" in docs/REALTIME_GATEWAY.md.
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
  private connected = false;

  constructor(options: ValkeyBusSubscriberOptions) {
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
  }

  subscribe(channel: string, listener: (message: string) => void): Promise<void> {
    return this.operationsByChannel.run(channel, () => this.cluster.sSubscribe(channel, listener));
  }

  unsubscribe(channel: string): Promise<void> {
    return this.operationsByChannel.run(channel, async () => {
      await this.cluster.sUnsubscribe(channel);
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
    await this.cluster.close();
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
