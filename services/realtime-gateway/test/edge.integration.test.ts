import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import {
  createGate,
  killSubscriberConnections,
  liveState,
  LiveStateStub,
  randomAuctionId,
  SseTestClient,
  startEdge,
  TestBus,
} from "./support/edge-harness.js";
import type { EdgeRun } from "./support/edge-harness.types.js";
import { waitFor } from "./support/relay-harness.js";

const valkeyNodes = process.env.TEST_VALKEY_NODES?.split(",");

// The edge against the real Valkey cluster. Events are published straight to
// the bus, standing in for the relay, and live states come from a stub of
// auction-service whose reads a test can hold. The path through the real relay
// and auction-service is the root end-to-end suite, tests/e2e.
describe.skipIf(valkeyNodes === undefined)("SSE edge", { timeout: 30_000 }, () => {
  const lingerMs = 1_000;
  let stub: LiveStateStub;
  let bus: TestBus;
  let edge: EdgeRun;
  let clients: SseTestClient[] = [];

  beforeAll(async () => {
    stub = new LiveStateStub();
    await stub.start();
    bus = new TestBus(valkeyNodes!);
    await bus.connect();
    edge = await startEdge({
      valkeyNodes: valkeyNodes!,
      auctionServiceUrl: stub.url,
      lingerMs,
      heartbeatIntervalMs: 200,
      // Long enough that the loads of concurrent first watchers share a
      // request even on a slow machine.
      liveStateBatchDelayMs: 50,
    });
  });

  afterEach(() => {
    for (const client of clients) {
      client.close();
    }
    clients = [];
    stub.beforeRead = async () => {};
    stub.afterRead = async () => {};
  });

  afterAll(async () => {
    await edge?.stop();
    await bus?.close();
    await stub?.stop();
  });

  async function connect(...auctionIds: string[]): Promise<SseTestClient> {
    const client = await SseTestClient.open(
      `${edge.url}/v1/stream?auctions=${auctionIds.join(",")}`,
    );
    clients.push(client);
    return client;
  }

  it("sends a snapshot per auction on connect, then accepted bids in version order", async () => {
    const withBids = randomAuctionId();
    const withoutBids = randomAuctionId();
    stub.states.set(withBids, liveState(withBids, 3));
    stub.states.set(withoutBids, liveState(withoutBids, 1));

    const client = await connect(withBids, withoutBids);
    await client.waitForTimeline(withBids, ["snapshot@3"]);
    await client.waitForTimeline(withoutBids, ["snapshot@1"]);
    await bus.publishBid(withBids, 4);
    await client.waitForTimeline(withBids, ["snapshot@3", "bid-accepted@4"]);

    expect(client.response.statusCode).toBe(200);
    expect(client.response.headers["content-type"]).toBe("text/event-stream; charset=utf-8");
    const [snapshot, bid] = client.messages.filter(({ data }) => data.auctionId === withBids);
    expect(snapshot).toEqual({
      event: "snapshot",
      data: {
        auctionId: withBids,
        auctionVersion: "3",
        phase: "open",
        currencyCode: "USD",
        startsAt: "2030-01-01T09:00:00.000Z",
        endsAt: "2030-01-01T12:00:00.000Z",
        cancelledAt: null,
        closedAt: null,
        leadingBid: { bidId: "1003", amountMinor: "300" },
        serverTime: expect.any(String),
      },
    });
    expect(bid).toEqual({
      event: "bid-accepted",
      data: {
        auctionId: withBids,
        auctionVersion: "4",
        bidId: "1004",
        amountMinor: "400",
        currencyCode: "USD",
        occurredAt: "2030-01-01T10:00:00.000000Z",
      },
    });
    expect(client.timeline(withoutBids)).toEqual(["snapshot@1"]);
  });

  it("drops duplicates and late copies of events it already applied", async () => {
    const auctionId = randomAuctionId();
    stub.states.set(auctionId, liveState(auctionId, 1));
    const client = await connect(auctionId);
    await client.waitForTimeline(auctionId, ["snapshot@1"]);
    const before = edge.registry.stats();

    // Kafka redelivers an event five times, then the outbox's v, v+1, v.
    await bus.publishBid(auctionId, 2, 5);
    await bus.publishBid(auctionId, 3);
    await bus.publishBid(auctionId, 2);
    await bus.publishBid(auctionId, 4);
    await client.waitForTimeline(auctionId, [
      "snapshot@1",
      "bid-accepted@2",
      "bid-accepted@3",
      "bid-accepted@4",
    ]);

    expect(client.timeline(auctionId)).toEqual([
      "snapshot@1",
      "bid-accepted@2",
      "bid-accepted@3",
      "bid-accepted@4",
    ]);
    const after = edge.registry.stats();
    expect(after.eventsApplied - before.eventsApplied).toBe(3);
    expect(after.eventsDropped - before.eventsDropped).toBe(5);
  });

  it("resyncs an auction on a version gap and then drops the delayed event", async () => {
    const auctionId = randomAuctionId();
    stub.states.set(auctionId, liveState(auctionId, 1));
    const client = await connect(auctionId);
    await client.waitForTimeline(auctionId, ["snapshot@1"]);
    const before = edge.registry.stats();
    const requestsBefore = stub.requests.length;

    // Version 2 is committed but delayed on the bus (a relay rebalance), so
    // version 3 arrives first.
    stub.states.set(auctionId, liveState(auctionId, 3));
    await bus.publishBid(auctionId, 3);
    await client.waitForTimeline(auctionId, ["snapshot@1", "snapshot@3"]);
    await bus.publishBid(auctionId, 2);
    await bus.publishBid(auctionId, 4);
    await client.waitForTimeline(auctionId, ["snapshot@1", "snapshot@3", "bid-accepted@4"]);

    expect(client.timeline(auctionId)).toEqual(["snapshot@1", "snapshot@3", "bid-accepted@4"]);
    expect(client.messages.at(-2)?.data.leadingBid).toEqual({ bidId: "1003", amountMinor: "300" });
    expect(stub.idsRequestedSince(requestsBefore)).toEqual([auctionId]);
    const after = edge.registry.stats();
    expect(after.resyncs.gap - before.resyncs.gap).toBe(1);
    // The version 3 that revealed the gap, buffered and then covered by the
    // read, and the delayed version 2.
    expect(after.eventsDropped - before.eventsDropped).toBe(2);
  });

  // The first subscriber subscribes, then reads, so an event committed
  // during the read is either in the read or buffered. Either way it must be
  // applied exactly once. The gate holds the read on either side of it.
  it.each([
    ["committed before the read, so the read reflects it", "before" as const],
    ["committed after the read, so only the buffer holds it", "after" as const],
  ])(
    "applies an event published during the first subscriber's read exactly once: %s",
    async (_case, commit) => {
      const auctionId = randomAuctionId();
      stub.states.set(auctionId, liveState(auctionId, 1));
      const gate = createGate();
      const hold = (ids: readonly string[]) =>
        ids.includes(auctionId) ? gate.pass() : Promise.resolve();
      if (commit === "before") {
        stub.beforeRead = hold;
      } else {
        stub.afterRead = hold;
      }
      const before = edge.registry.stats();

      const client = await connect(auctionId);
      await gate.entered;
      // The edge is subscribed before it reads, so this reaches its buffer.
      stub.states.set(auctionId, liveState(auctionId, 2));
      await bus.publishBid(auctionId, 2);
      await waitFor(
        () => edge.registry.stats().eventsBuffered - before.eventsBuffered === 1,
        "the event to be buffered",
      );
      gate.open();
      await client.waitForTimeline(auctionId, ["snapshot@2"]);
      await bus.publishBid(auctionId, 3);
      await client.waitForTimeline(auctionId, ["snapshot@2", "bid-accepted@3"]);

      expect(client.timeline(auctionId)).toEqual(["snapshot@2", "bid-accepted@3"]);
      const after = edge.registry.stats();
      // Version 2 is applied once: by the read, or from the buffer.
      expect({
        appliedFromBuffer: after.eventsApplied - before.eventsApplied - 1,
        droppedAsReflected: after.eventsDropped - before.eventsDropped,
      }).toEqual(
        commit === "before"
          ? { appliedFromBuffer: 0, droppedAsReflected: 1 }
          : { appliedFromBuffer: 1, droppedAsReflected: 0 },
      );
    },
  );

  it("shares one batched read per auction between clients and serves later clients from memory", async () => {
    const auctionIds = [randomAuctionId(), randomAuctionId(), randomAuctionId()];
    for (const id of auctionIds) {
      stub.states.set(id, liveState(id, 1));
    }
    const requestsBefore = stub.requests.length;

    const [first, second] = await Promise.all([connect(...auctionIds), connect(...auctionIds)]);
    for (const client of [first!, second!]) {
      for (const id of auctionIds) {
        await client.waitForTimeline(id, ["snapshot@1"]);
      }
    }
    const late = await connect(auctionIds[1]!);
    await late.waitForTimeline(auctionIds[1]!, ["snapshot@1"]);

    expect(stub.requests.length - requestsBefore).toBe(1);
    expect(stub.idsRequestedSince(requestsBefore).toSorted()).toEqual(auctionIds.toSorted());
  });

  it("reports an auction that does not exist as unavailable", async () => {
    const auctionId = randomAuctionId();

    const client = await connect(auctionId);
    await client.waitForTimeline(auctionId, ["unavailable"]);

    expect(client.messages).toEqual([
      { event: "unavailable", data: { auctionId, reason: "not-found" } },
    ]);
  });

  it("sends heartbeat comments on a quiet stream", async () => {
    const auctionId = randomAuctionId();
    stub.states.set(auctionId, liveState(auctionId, 1));

    const client = await connect(auctionId);
    await waitFor(() => client.comments.length >= 2, "two heartbeats");

    expect(client.comments.slice(0, 2)).toEqual(["heartbeat", "heartbeat"]);
  });

  it("keeps an auction subscribed for the linger period after its last client leaves", async () => {
    const auctionId = randomAuctionId();
    stub.states.set(auctionId, liveState(auctionId, 1));
    const first = await connect(auctionId);
    await first.waitForTimeline(auctionId, ["snapshot@1"]);
    const requestsAfterFirst = stub.requests.length;
    const applied = edge.registry.stats().eventsApplied;

    // Within the linger period, a returning client is served from memory,
    // including a change applied while nobody watched.
    first.close();
    await waitFor(() => first.ended, "the first client to disconnect");
    stub.states.set(auctionId, liveState(auctionId, 2));
    await bus.publishBid(auctionId, 2);
    await waitFor(() => edge.registry.stats().eventsApplied > applied, "the change to be applied");
    const returning = await connect(auctionId);
    await returning.waitForTimeline(auctionId, ["snapshot@2"]);
    expect(stub.requests.length).toBe(requestsAfterFirst);

    // After it, the auction is unsubscribed and dropped, and the next client
    // reads it again.
    returning.close();
    await waitFor(() => edge.registry.stats().auctions === 0, "every auction to be dropped");
    const later = await connect(auctionId);
    await later.waitForTimeline(auctionId, ["snapshot@2"]);
    expect(stub.idsRequestedSince(requestsAfterFirst)).toEqual([auctionId]);
  });

  it.each([
    ["no auctions parameter", ""],
    ["an empty list", "?auctions="],
    ["a non-numeric id", "?auctions=12,abc"],
    ["a zero id", "?auctions=0"],
    ["an id beyond bigint", "?auctions=9223372036854775808"],
    ["51 auctions", `?auctions=${Array.from({ length: 51 }, (_, index) => index + 1).join(",")}`],
    ["an unknown parameter", "?auctions=1&since=5"],
  ])("rejects a stream request with %s", async (_case, query) => {
    const response = await fetch(`${edge.url}/v1/stream${query}`);

    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({
      code: "INVALID_REQUEST",
      message: "auctions must be 1 to 50 comma-separated auction IDs",
    });
  });
});

describe.skipIf(valkeyNodes === undefined)(
  "SSE edge after losing its Valkey connections",
  { timeout: 30_000 },
  () => {
    let stub: LiveStateStub;
    let bus: TestBus;
    let edge: EdgeRun;
    let client: SseTestClient | undefined;

    beforeAll(async () => {
      stub = new LiveStateStub();
      await stub.start();
      bus = new TestBus(valkeyNodes!);
      await bus.connect();
      edge = await startEdge({ valkeyNodes: valkeyNodes!, auctionServiceUrl: stub.url });
    });

    afterAll(async () => {
      client?.close();
      await edge?.stop();
      await bus?.close();
      await stub?.stop();
    });

    it("resyncs every watched auction once resubscribed, then delivers events again", async () => {
      // Spread over the cluster, so the edge holds several subscriber
      // connections.
      const auctionIds = Array.from({ length: 12 }, () => randomAuctionId());
      for (const id of auctionIds) {
        stub.states.set(id, liveState(id, 1));
      }
      client = await SseTestClient.open(`${edge.url}/v1/stream?auctions=${auctionIds.join(",")}`);
      for (const id of auctionIds) {
        await client.waitForTimeline(id, ["snapshot@1"]);
      }

      // Changes the edge never hears of, as if published while it was
      // disconnected.
      for (const id of auctionIds) {
        stub.states.set(id, liveState(id, 2));
      }
      const before = edge.registry.stats();
      const killed = await killSubscriberConnections(valkeyNodes!, edge.clientName);
      expect(killed).toBeGreaterThan(1);

      // Every lost connection resyncs every auction, and a resync ends only
      // after every connection resubscribed.
      await waitFor(() => {
        const stats = edge.registry.stats();
        const resyncs = stats.resyncs["bus-interrupted"] - before.resyncs["bus-interrupted"];
        return resyncs === killed * auctionIds.length && stats.syncing === 0;
      }, "every resync to finish");
      for (const id of auctionIds) {
        stub.states.set(id, liveState(id, 3));
        await bus.publishBid(id, 3);
      }
      for (const id of auctionIds) {
        await waitFor(
          () => client!.timeline(id).at(-1) === "bid-accepted@3",
          `bid-accepted@3 for ${id}; got ${client!.timeline(id).join(", ")}`,
        );
        // A resync that finished before the next connection was lost sends
        // its own snapshot, so the current state may arrive more than once.
        const [first, ...rest] = client.timeline(id);
        expect(first).toBe("snapshot@1");
        expect(rest.at(-1)).toBe("bid-accepted@3");
        expect(new Set(rest.slice(0, -1))).toEqual(new Set(["snapshot@2"]));
      }
    });
  },
);
