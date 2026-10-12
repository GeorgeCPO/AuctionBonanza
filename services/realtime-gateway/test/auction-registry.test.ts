import { pino } from "pino";
import { describe, expect, it } from "vitest";

import { auctionChannel, serializeAuctionEvent } from "../src/bus/bus.js";
import type { BusSubscriber } from "../src/bus/bus.types.js";
import { AuctionRegistry } from "../src/edge/auction-registry.js";
import type { AuctionWatcher } from "../src/edge/auction-registry.types.js";
import type { LiveStateLookup, LiveStateSource } from "../src/edge/live-states.types.js";
import { bidAcceptedEvent, liveState } from "./support/edge-harness.js";
import { waitFor } from "./support/relay-harness.js";

// The registry's timing rules with the bus and auction-service replaced by
// fakes the test steps through. The same rules run against Valkey in
// edge.integration.test.ts; the races here are hard to stage there.

class FakeBus implements BusSubscriber {
  readonly listeners = new Map<string, (message: string) => void>();
  readonly subscribed: string[] = [];
  readonly unsubscribed: string[] = [];
  private interruptionListener: () => void = () => {};
  private subscriptionLostListener: (channel: string) => void = () => {};
  private restored: PromiseWithResolvers<void> | undefined;

  async subscribe(channel: string, listener: (message: string) => void): Promise<void> {
    this.listeners.set(channel, listener);
    this.subscribed.push(channel);
  }

  async unsubscribe(channel: string): Promise<void> {
    this.listeners.delete(channel);
    this.unsubscribed.push(channel);
  }

  onInterruption(listener: () => void): void {
    this.interruptionListener = listener;
  }

  onSubscriptionLost(listener: (channel: string) => void): void {
    this.subscriptionLostListener = listener;
  }

  whenRestored(): Promise<void> {
    return this.restored?.promise ?? Promise.resolve();
  }

  publishBid(auctionId: string, version: number): void {
    this.publishRaw(
      auctionId,
      serializeAuctionEvent("BidAccepted.v1", bidAcceptedEvent(auctionId, version)),
    );
  }

  publishRaw(auctionId: string, message: string): void {
    this.listeners.get(auctionChannel(auctionId))?.(message);
  }

  loseConnection(): void {
    this.restored ??= Promise.withResolvers();
    this.interruptionListener();
  }

  // Like node-redis failing to follow a moved channel: the listener is gone.
  loseSubscription(auctionId: string): void {
    this.listeners.delete(auctionChannel(auctionId));
    this.subscriptionLostListener(auctionChannel(auctionId));
  }

  restoreConnection(): void {
    this.restored?.resolve();
    this.restored = undefined;
  }
}

// Each load waits until the test answers it.
class FakeLiveStates implements LiveStateSource {
  readonly loads: { auctionId: string; answer: PromiseWithResolvers<LiveStateLookup> }[] = [];

  load(auctionId: string): Promise<LiveStateLookup> {
    const answer = Promise.withResolvers<LiveStateLookup>();
    this.loads.push({ auctionId, answer });
    return answer.promise;
  }

  async answer(index: number, lookup: LiveStateLookup | Error): Promise<void> {
    await waitFor(() => this.loads.length > index, `load ${index}`, 2_000);
    const { answer } = this.loads[index]!;
    if (lookup instanceof Error) answer.reject(lookup);
    else answer.resolve(lookup);
  }
}

// Answers every load with the current lookup, or fails it, on the next turn
// of the event loop. Yielding there keeps a read loop failing the test
// instead of starving the timers that would end it.
class AnsweringLiveStates implements LiveStateSource {
  loads = 0;
  constructor(public lookup: LiveStateLookup | Error) {}

  async load(): Promise<LiveStateLookup> {
    this.loads += 1;
    await new Promise((resolve) => setImmediate(resolve));
    if (this.lookup instanceof Error) throw this.lookup;
    return this.lookup;
  }
}

class RecordingWatcher implements AuctionWatcher {
  readonly timeline: string[] = [];

  send(frame: string): void {
    const event = /^event: (.+)$/m.exec(frame)![1]!;
    const data = JSON.parse(/^data: (.+)$/m.exec(frame)![1]!) as { auctionVersion?: string };
    this.timeline.push(
      data.auctionVersion === undefined ? event : `${event}@${data.auctionVersion}`,
    );
  }
}

function found(auctionId: string, version: number): LiveStateLookup {
  return { kind: "found", state: liveState(auctionId, version) };
}

function setup<Source extends LiveStateSource = FakeLiveStates>(
  options: {
    lingerMs?: number;
    maxBufferedEvents?: number;
    retryBaseDelayMs?: number;
    liveStates?: Source;
  } = {},
) {
  const bus = new FakeBus();
  const liveStates = options.liveStates ?? (new FakeLiveStates() as LiveStateSource as Source);
  const retryBaseDelayMs = options.retryBaseDelayMs ?? 10;
  const registry = new AuctionRegistry({
    bus,
    liveStates,
    lingerMs: options.lingerMs ?? 60_000,
    maxBufferedEvents: options.maxBufferedEvents ?? 100,
    retryBackoff: { baseDelayMs: retryBaseDelayMs, maxDelayMs: retryBaseDelayMs * 2 },
    logger: pino({ level: "silent" }),
  });
  const watcher = new RecordingWatcher();
  return { bus, liveStates, registry, watcher };
}

// Lets pending promise callbacks run.
const settle = () => new Promise((resolve) => setImmediate(resolve));

describe("AuctionRegistry", () => {
  it("discards a read that started before a lost connection and reads again once resubscribed", async () => {
    const { bus, liveStates, registry, watcher } = setup();
    registry.watch("42", watcher);
    await waitFor(() => liveStates.loads.length === 1, "the first read");

    bus.loseConnection();
    // The first read answers with a state that may miss what was lost.
    await liveStates.answer(0, found("42", 1));
    await settle();
    expect(watcher.timeline).toEqual([]);
    expect(liveStates.loads).toHaveLength(1);

    bus.restoreConnection();
    await liveStates.answer(1, found("42", 2));
    await waitFor(() => watcher.timeline.length === 1, "the snapshot");
    expect(watcher.timeline).toEqual(["snapshot@2"]);
    expect(registry.stats().resyncs["bus-interrupted"]).toBe(1);
  });

  it("retries a failed read with backoff", async () => {
    const { liveStates, registry, watcher } = setup();
    registry.watch("42", watcher);

    await liveStates.answer(0, new Error("auction-service unavailable"));
    await liveStates.answer(1, found("42", 3));
    await waitFor(() => watcher.timeline.length === 1, "the snapshot");

    expect(watcher.timeline).toEqual(["snapshot@3"]);
  });

  it("folds events buffered during the read into the snapshot", async () => {
    const { bus, liveStates, registry, watcher } = setup();
    registry.watch("42", watcher);
    await waitFor(() => liveStates.loads.length === 1, "the first read");

    bus.publishBid("42", 2);
    bus.publishBid("42", 3);
    bus.publishBid("42", 3);
    await liveStates.answer(0, found("42", 1));
    await waitFor(() => watcher.timeline.length === 1, "the snapshot");
    bus.publishBid("42", 4);

    expect(watcher.timeline).toEqual(["snapshot@3", "bid-accepted@4"]);
    expect(registry.stats()).toMatchObject({
      eventsBuffered: 3,
      eventsApplied: 3,
      eventsDropped: 1,
    });
  });

  it("reads again when the buffered events leave a gap after the read", async () => {
    const { bus, liveStates, registry, watcher } = setup();
    registry.watch("42", watcher);
    await waitFor(() => liveStates.loads.length === 1, "the first read");

    bus.publishBid("42", 4);
    await liveStates.answer(0, found("42", 2));
    await liveStates.answer(1, found("42", 4));
    await waitFor(() => watcher.timeline.length === 1, "the snapshot");

    expect(watcher.timeline).toEqual(["snapshot@4"]);
    expect(registry.stats().resyncs.gap).toBe(1);
  });

  // With a 1 s base delay, every backed-off read waits at least 500 ms.
  it("backs off instead of reading in a loop when a read cannot close a gap", async () => {
    const liveStates = new AnsweringLiveStates({ kind: "missing" });
    const { bus, registry, watcher } = setup({ liveStates, retryBaseDelayMs: 1_000 });
    registry.watch("42", watcher);
    await waitFor(() => watcher.timeline.length === 1, "the unavailable message");

    // A read that lags the bus: it keeps answering "missing" for an auction
    // the bus already shows at version 2.
    bus.publishBid("42", 2);
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(liveStates.loads).toBe(2);
    expect(watcher.timeline).toEqual(["unavailable"]);

    // Once a read settles the auction, a new gap is repaired at once again.
    liveStates.lookup = found("42", 2);
    await waitFor(() => watcher.timeline.length === 2, "the snapshot", 3_000);
    liveStates.lookup = found("42", 4);
    bus.publishBid("42", 4);
    await waitFor(() => watcher.timeline.length === 3, "the next snapshot", 300);

    expect(watcher.timeline).toEqual(["unavailable", "snapshot@2", "snapshot@4"]);
  });

  it("keeps backing off when the buffer overflows while auction-service is down", async () => {
    const liveStates = new AnsweringLiveStates(new Error("auction-service unavailable"));
    const { bus, registry, watcher } = setup({
      liveStates,
      retryBaseDelayMs: 1_000,
      maxBufferedEvents: 2,
    });
    registry.watch("42", watcher);
    await waitFor(() => liveStates.loads === 1, "the first read");

    for (const version of [2, 3, 4]) bus.publishBid("42", version);
    await new Promise((resolve) => setTimeout(resolve, 300));

    expect(liveStates.loads).toBe(1);
    expect(registry.stats().resyncs["buffer-overflow"]).toBe(1);
  });

  it("subscribes again and resyncs when the bus drops a channel's listener", async () => {
    const { bus, liveStates, registry, watcher } = setup();
    registry.watch("42", watcher);
    await liveStates.answer(0, found("42", 1));
    await waitFor(() => watcher.timeline.length === 1, "the snapshot");

    bus.loseSubscription("42");
    await liveStates.answer(1, found("42", 2));
    await waitFor(() => watcher.timeline.length === 2, "the fresh snapshot");
    bus.publishBid("42", 3);

    expect(bus.subscribed).toEqual(["auction:42", "auction:42"]);
    expect(watcher.timeline).toEqual(["snapshot@1", "snapshot@2", "bid-accepted@3"]);
    expect(registry.stats().resyncs["subscription-lost"]).toBe(1);
  });

  it("discards an overflowing buffer and reads again", async () => {
    const { bus, liveStates, registry, watcher } = setup({ maxBufferedEvents: 2 });
    registry.watch("42", watcher);
    await waitFor(() => liveStates.loads.length === 1, "the first read");

    for (const version of [2, 3, 4]) bus.publishBid("42", version);
    // The first read may predate the discarded events, so it is ignored.
    await liveStates.answer(0, found("42", 1));
    await liveStates.answer(1, found("42", 4));
    await waitFor(() => watcher.timeline.length === 1, "the snapshot");

    expect(watcher.timeline).toEqual(["snapshot@4"]);
    expect(registry.stats().resyncs["buffer-overflow"]).toBe(1);
  });

  it("resyncs on an event type it cannot apply, and drops one it already reflects", async () => {
    const { bus, liveStates, registry, watcher } = setup();
    registry.watch("42", watcher);
    await liveStates.answer(0, found("42", 2));
    await waitFor(() => watcher.timeline.length === 1, "the snapshot");
    const unsupported = (version: number) =>
      JSON.stringify({
        v: 1,
        eventType: "AuctionCancelled.v1",
        event: { auctionId: "42", auctionVersion: String(version) },
      });

    bus.publishRaw("42", unsupported(2));
    bus.publishRaw("42", unsupported(3));
    await liveStates.answer(1, found("42", 3));
    await waitFor(() => watcher.timeline.length === 2, "the second snapshot");

    expect(watcher.timeline).toEqual(["snapshot@2", "snapshot@3"]);
    expect(registry.stats()).toMatchObject({ eventsDropped: 2, resyncs: { gap: 1 } });
  });

  it("ignores malformed messages and messages for another auction", async () => {
    const { bus, liveStates, registry, watcher } = setup();
    registry.watch("42", watcher);
    await liveStates.answer(0, found("42", 1));
    await waitFor(() => watcher.timeline.length === 1, "the snapshot");

    bus.publishRaw("42", "not json");
    bus.publishRaw("42", JSON.stringify({ v: 2, eventType: "BidAccepted.v1", event: {} }));
    bus.publishRaw("42", serializeAuctionEvent("BidAccepted.v1", bidAcceptedEvent("43", 2)));
    bus.publishBid("42", 2);

    expect(watcher.timeline).toEqual(["snapshot@1", "bid-accepted@2"]);
    expect(registry.stats().malformedMessages).toBe(3);
  });

  it("reports a missing auction and reads it again when an event shows it exists", async () => {
    const { bus, liveStates, registry, watcher } = setup();
    registry.watch("42", watcher);
    await liveStates.answer(0, { kind: "missing" });
    await waitFor(() => watcher.timeline.length === 1, "the unavailable message");

    bus.publishBid("42", 2);
    await liveStates.answer(1, found("42", 2));
    await waitFor(() => watcher.timeline.length === 2, "the snapshot");

    expect(watcher.timeline).toEqual(["unavailable", "snapshot@2"]);
  });

  it("serves a watcher joining during a resync the last state, then the fresh one", async () => {
    const { bus, liveStates, registry, watcher } = setup();
    registry.watch("42", watcher);
    await liveStates.answer(0, found("42", 1));
    await waitFor(() => watcher.timeline.length === 1, "the snapshot");

    bus.publishBid("42", 3);
    const joining = new RecordingWatcher();
    registry.watch("42", joining);
    await liveStates.answer(1, found("42", 3));
    await waitFor(() => joining.timeline.length === 2, "the fresh snapshot");

    expect(joining.timeline).toEqual(["snapshot@1", "snapshot@3"]);
    expect(watcher.timeline).toEqual(["snapshot@1", "snapshot@3"]);
  });

  it("unsubscribes only after the linger period, and not when a watcher returns", async () => {
    const { bus, liveStates, registry, watcher } = setup({ lingerMs: 50 });
    registry.watch("42", watcher);
    await liveStates.answer(0, found("42", 1));
    await waitFor(() => watcher.timeline.length === 1, "the snapshot");

    registry.unwatch("42", watcher);
    registry.watch("42", watcher);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(bus.unsubscribed).toEqual([]);
    expect(watcher.timeline).toEqual(["snapshot@1", "snapshot@1"]);

    registry.unwatch("42", watcher);
    await waitFor(() => bus.unsubscribed.length === 1, "the unsubscribe");
    expect(bus.unsubscribed).toEqual(["auction:42"]);
    expect(registry.stats().auctions).toBe(0);
  });
});
