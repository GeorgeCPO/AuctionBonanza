import { randomInt, randomUUID } from "node:crypto";
import { once } from "node:events";
import {
  createServer,
  get,
  type ClientRequest,
  type IncomingMessage,
  type Server,
} from "node:http";
import type { AddressInfo } from "node:net";

import type { BidAcceptedV1 } from "@auction-bonanza/contracts";
import { pino } from "pino";
import { createClient } from "redis";

import { auctionChannel, serializeAuctionEvent } from "../../src/bus/bus.js";
import { ValkeyBusPublisher } from "../../src/bus/valkey-bus-publisher.js";
import { ValkeyBusSubscriber } from "../../src/bus/valkey-bus-subscriber.js";
import { AuctionRegistry } from "../../src/edge/auction-registry.js";
import { EdgeServer } from "../../src/edge/edge-server.js";
import {
  LiveStateBatcher,
  LiveStateClient,
  type AuctionLiveState,
} from "../../src/edge/live-states.js";
import { waitFor } from "./relay-harness.js";
import type { Gate, SseMessage, EdgeRun, StartEdgeOptions } from "./edge-harness.types.js";

// Auction ids far apart from other test files, which share the Valkey
// cluster and its channels.
export function randomAuctionId(): string {
  return String(randomInt(1_000_000_000, 2 ** 47));
}

export function bidAcceptedEvent(auctionId: string, version: number): BidAcceptedV1 {
  return {
    eventId: randomUUID(),
    eventType: "BidAccepted.v1",
    occurredAt: "2030-01-01T10:00:00.000000Z",
    auctionId,
    auctionVersion: String(version),
    bidId: String(1000 + version),
    amountMinor: String(100 * version),
    currencyCode: "USD",
  };
}

// The live state auction-service would return after `version - 1` accepted
// bids, matching bidAcceptedEvent.
export function liveState(auctionId: string, version: number): AuctionLiveState {
  return {
    auctionId,
    auctionVersion: String(version),
    currencyCode: "USD",
    startsAt: "2030-01-01T09:00:00.000Z",
    endsAt: "2030-01-01T12:00:00.000Z",
    cancelledAt: null,
    closedAt: null,
    phase: "open",
    leadingBid:
      version === 1 ? null : { bidId: String(1000 + version), amountMinor: String(100 * version) },
  };
}

// Publishes BidAccepted.v1 envelopes straight to the bus, standing in for the
// relay so tests control exactly which versions an edge sees.
export class TestBus {
  private readonly publisher: ValkeyBusPublisher;

  constructor(nodes: readonly string[]) {
    this.publisher = new ValkeyBusPublisher({
      nodes,
      commandTimeoutMs: 2_000,
      logger: pino({ level: "silent" }),
    });
  }

  connect(): Promise<void> {
    return this.publisher.connect();
  }

  async publishBid(auctionId: string, version: number, times = 1): Promise<void> {
    const message = serializeAuctionEvent("BidAccepted.v1", bidAcceptedEvent(auctionId, version));
    for (let copy = 0; copy < times; copy += 1) {
      await this.publisher.publish(auctionChannel(auctionId), message);
    }
  }

  async publishRaw(auctionId: string, message: string): Promise<void> {
    await this.publisher.publish(auctionChannel(auctionId), message);
  }

  close(): Promise<void> {
    return this.publisher.close();
  }
}

export function createGate(): Gate {
  const entered = Promise.withResolvers<void>();
  const opened = Promise.withResolvers<void>();
  return {
    entered: entered.promise,
    pass: async () => {
      entered.resolve();
      await opened.promise;
    },
    open: () => opened.resolve(),
  };
}

// Serves GET /v1/auctions/live-states from an in-memory map, in the shape
// auction-service uses. Hooks hold a request before or after the map is read.
export class LiveStateStub {
  readonly states = new Map<string, AuctionLiveState>();
  readonly requests: string[][] = [];
  beforeRead: (ids: readonly string[]) => Promise<void> = async () => {};
  afterRead: (ids: readonly string[]) => Promise<void> = async () => {};
  private readonly server: Server;

  constructor() {
    this.server = createServer((request, response) => {
      void (async () => {
        const url = new URL(request.url ?? "/", "http://stub");
        const ids = (url.searchParams.get("ids") ?? "").split(",");
        this.requests.push(ids);
        await this.beforeRead(ids);
        const items = ids.flatMap((id) => {
          const state = this.states.get(id);
          return state === undefined ? [] : [structuredClone(state)];
        });
        const missingIds = ids.filter((id) => !this.states.has(id));
        await this.afterRead(ids);
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ items, missingIds, observedAt: new Date().toISOString() }));
      })();
    });
  }

  get url(): string {
    const { port } = this.server.address() as AddressInfo;
    return `http://127.0.0.1:${port}`;
  }

  // Every id requested since the given request index, in order.
  idsRequestedSince(requestIndex: number): string[] {
    return this.requests.slice(requestIndex).flat();
  }

  async start(): Promise<void> {
    this.server.listen(0, "127.0.0.1");
    await once(this.server, "listening");
  }

  async stop(): Promise<void> {
    this.server.closeAllConnections();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

// Reads an SSE response the way a browser's EventSource would, keeping every
// message and comment.
export class SseTestClient {
  readonly messages: SseMessage[] = [];
  readonly comments: string[] = [];
  ended = false;
  private pending = "";

  private constructor(
    private readonly request: ClientRequest,
    readonly response: IncomingMessage,
  ) {
    response.setEncoding("utf8");
    response.on("data", (chunk: string) => this.receive(chunk));
    response.on("close", () => (this.ended = true));
  }

  static open(url: string): Promise<SseTestClient> {
    return new Promise((resolve, reject) => {
      const request = get(url, { headers: { accept: "text/event-stream" } }, (response) =>
        resolve(new SseTestClient(request, response)),
      );
      request.on("error", reject);
    });
  }

  // "event@version" for each message about the auction, in arrival order.
  timeline(auctionId: string): string[] {
    return this.messages
      .filter(({ data }) => data.auctionId === auctionId)
      .map(({ event, data }) =>
        typeof data.auctionVersion === "string" ? `${event}@${data.auctionVersion}` : event,
      );
  }

  async waitForTimeline(auctionId: string, expected: readonly string[]): Promise<void> {
    await waitFor(
      () => this.timeline(auctionId).length >= expected.length,
      `${expected.join(", ")} for auction ${auctionId}; got ${this.timeline(auctionId).join(", ")}`,
      10_000,
    );
  }

  close(): void {
    this.request.destroy();
  }

  private receive(chunk: string): void {
    this.pending += chunk;
    let end = this.pending.indexOf("\n\n");
    while (end !== -1) {
      const block = this.pending.slice(0, end);
      this.pending = this.pending.slice(end + 2);
      let event = "message";
      let data: string | undefined;
      for (const line of block.split("\n")) {
        if (line.startsWith(":")) {
          this.comments.push(line.slice(1).trim());
        } else if (line.startsWith("event: ")) {
          event = line.slice("event: ".length);
        } else if (line.startsWith("data: ")) {
          data = line.slice("data: ".length);
        }
      }
      if (data !== undefined) {
        this.messages.push({ event, data: JSON.parse(data) as Record<string, unknown> });
      }
      end = this.pending.indexOf("\n\n");
    }
  }
}

// Runs the edge in this process, wired like edge.main.ts, so tests can read
// its registry's counters.
export async function startEdge(options: StartEdgeOptions): Promise<EdgeRun> {
  const logger = options.logger ?? pino({ level: "silent" });
  const clientName = `edge-test-${randomUUID()}`;
  const bus = new ValkeyBusSubscriber({
    nodes: options.valkeyNodes,
    clientName,
    connectTimeoutMs: 2_000,
    logger,
  });
  await bus.connect();
  const registry = new AuctionRegistry({
    bus,
    liveStates: new LiveStateBatcher({
      client: new LiveStateClient({
        auctionServiceUrl: options.auctionServiceUrl,
        timeoutMs: 5_000,
      }),
      batchDelayMs: options.liveStateBatchDelayMs ?? 5,
      maxConcurrentRequests: 4,
    }),
    maxWatchedAuctions: 100_000,
    lingerMs: options.lingerMs ?? 30_000,
    maxBufferedEvents: 1_000,
    retryBackoff: { baseDelayMs: 50, maxDelayMs: 200 },
    logger,
  });
  const server = new EdgeServer({
    registry,
    maxAuctionsPerStream: 50,
    maxConnections: 10_000,
    heartbeatIntervalMs: options.heartbeatIntervalMs ?? 15_000,
    maxBufferedBytesPerConnection: 1024 * 1024,
    logger,
  });
  const { port } = await server.listen("127.0.0.1", 0);
  return {
    url: `http://127.0.0.1:${port}`,
    registry,
    clientName,
    stop: async () => {
      await server.close();
      registry.close();
      await bus.close();
    },
  };
}

// Closes the edge's subscriber connections from the server side, the way a
// Valkey restart, a network failure, or an exceeded output buffer would.
// Only the named client's connections are killed: other test files share
// the cluster.
export async function killSubscriberConnections(
  nodes: readonly string[],
  clientName: string,
): Promise<number> {
  let killed = 0;
  for (const node of nodes) {
    const client = createClient({ url: `redis://${node}` });
    await client.connect();
    try {
      for (const { id, name } of await client.clientList({ TYPE: "PUBSUB" })) {
        if (name !== clientName) {
          continue;
        }
        await client.clientKill({ filter: "ID", id });
        killed += 1;
      }
    } finally {
      await client.close();
    }
  }
  return killed;
}
