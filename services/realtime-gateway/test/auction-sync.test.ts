import { describe, expect, it } from "vitest";

import { AuctionSync } from "../src/edge/auction-sync.js";
import type { BusAuctionEvent } from "../src/bus/bus.types.js";
import type { LiveStateLookup } from "../src/edge/live-states.types.js";
import { bidAcceptedEvent, liveState } from "./support/edge-harness.js";

// AuctionSync owns its state, so every test sets up a situation through its
// public inputs, the way the registry does, and then checks the effects it
// returns and the state it exposes. Events and reads are frozen: if the class
// changed an input instead of building its own state, it would throw.

const AUCTION = "42";
const MAX_BUFFERED = 3;

function freeze<T>(value: T): T {
  if (typeof value === "object" && value !== null && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) freeze(child);
  }
  return value;
}

function bid(version: number): BusAuctionEvent {
  return freeze({
    kind: "bid-accepted",
    auctionId: AUCTION,
    version: BigInt(version),
    event: bidAcceptedEvent(AUCTION, version),
  });
}

function unsupported(version: number): BusAuctionEvent {
  return freeze({
    kind: "unsupported",
    auctionId: AUCTION,
    version: BigInt(version),
    eventType: "AuctionCancelled.v1",
  });
}

function found(version: number): LiveStateLookup {
  return freeze({ kind: "found", state: liveState(AUCTION, version) });
}

const missing: LiveStateLookup = freeze({ kind: "missing" });

// Started, with these events buffered while it waits for its first read.
function syncingWith(...events: BusAuctionEvent[]): AuctionSync {
  const sync = new AuctionSync(MAX_BUFFERED);
  sync.begin();
  for (const event of events) sync.onBusEvent(event);
  return sync;
}

// Started, and its first read, at this version, finished with nothing buffered.
function liveAt(version: number): AuctionSync {
  const sync = syncingWith();
  sync.onLiveStateRead(found(version));
  return sync;
}

const versionsOf = (events: readonly BusAuctionEvent[]) => events.map((e) => Number(e.version));
const kinds = (effects: readonly { kind: string }[]) => effects.map((effect) => effect.kind);
const counts = (effects: readonly { kind: string; counter?: string }[]) =>
  effects.filter((e) => e.kind === "count").map((e) => e.counter);

describe("begin", () => {
  it("starts syncing with nothing known, and asks for the first read", () => {
    const sync = new AuctionSync(MAX_BUFFERED);

    expect(sync.status).toBe("syncing");
    expect(sync.known).toBeUndefined();
    expect(sync.version).toBe(0n);
    expect(sync.bufferedEvents).toEqual([]);
    expect(sync.begin()).toEqual([{ kind: "start-sync", reason: "initial", afterBackoff: false }]);
  });

  it("keeps each instance's state to itself", () => {
    const first = liveAt(5);
    const second = syncingWith();

    expect(first.status).toBe("live");
    expect(second.status).toBe("syncing");
    expect(second.version).toBe(0n);
  });
});

describe("onBusEvent while syncing", () => {
  it("buffers events in arrival order and sends nothing", () => {
    const sync = syncingWith(bid(3));

    const effects = sync.onBusEvent(bid(2));

    expect(sync.status).toBe("syncing");
    expect(versionsOf(sync.bufferedEvents)).toEqual([3, 2]);
    expect(counts(effects)).toEqual(["eventsBuffered"]);
    expect(kinds(effects)).toEqual(["count"]);
  });

  it("discards the whole buffer on overflow and reads again after a backoff", () => {
    const sync = syncingWith(bid(2), bid(3), bid(4));

    const effects = sync.onBusEvent(bid(5));

    expect(sync.bufferedEvents).toEqual([]);
    expect(effects).toEqual([
      { kind: "count", counter: "eventsBuffered" },
      { kind: "start-sync", reason: "buffer-overflow", afterBackoff: true },
    ]);
  });
});

describe("onLiveStateRead", () => {
  it("goes live at the read's version when nothing was buffered", () => {
    const sync = syncingWith();

    const effects = sync.onLiveStateRead(found(7));

    expect(sync.status).toBe("live");
    expect(sync.version).toBe(7n);
    expect(sync.known).toEqual({ kind: "found", state: liveState(AUCTION, 7) });
    expect(effects).toEqual([{ kind: "synced" }]);
  });

  it("folds buffered events into the read: applies the newer, drops the rest, sends none", () => {
    const sync = syncingWith(bid(2), bid(3), bid(3));

    const effects = sync.onLiveStateRead(found(1));

    expect(sync.status).toBe("live");
    expect(sync.version).toBe(3n);
    expect(sync.bufferedEvents).toEqual([]);
    expect(sync.known).toMatchObject({
      kind: "found",
      state: { auctionVersion: "3", leadingBid: { bidId: "1003", amountMinor: "300" } },
    });
    expect(counts(effects)).toEqual(["eventsApplied", "eventsApplied", "eventsDropped"]);
    expect(kinds(effects)).not.toContain("send-bid");
    expect(effects.at(-1)).toEqual({ kind: "synced" });
  });

  it("drops buffered events the read already reflects", () => {
    const sync = syncingWith(bid(2), bid(3));

    const effects = sync.onLiveStateRead(found(5));

    expect(sync.status).toBe("live");
    expect(sync.version).toBe(5n);
    expect(counts(effects)).toEqual(["eventsDropped", "eventsDropped"]);
  });

  it("stays syncing and keeps the buffer when the read is behind it", () => {
    const sync = syncingWith(bid(4), bid(5));

    const effects = sync.onLiveStateRead(found(2));

    expect(sync.status).toBe("syncing");
    expect(sync.version).toBe(2n);
    expect(versionsOf(sync.bufferedEvents)).toEqual([4, 5]);
    expect(effects).toEqual([{ kind: "start-sync", reason: "gap", afterBackoff: true }]);
  });

  it("keeps only the events from the gap on, after applying the ones before it", () => {
    const sync = syncingWith(bid(2), bid(4), bid(5));

    const effects = sync.onLiveStateRead(found(1));

    expect(sync.version).toBe(2n);
    expect(versionsOf(sync.bufferedEvents)).toEqual([4, 5]);
    expect(counts(effects)).toEqual(["eventsApplied"]);
  });

  it("reports a missing auction as known, at version 0", () => {
    const sync = syncingWith();

    const effects = sync.onLiveStateRead(missing);

    expect(sync.status).toBe("live");
    expect(sync.known).toEqual({ kind: "missing" });
    expect(sync.version).toBe(0n);
    expect(effects).toEqual([{ kind: "synced" }]);
  });

  it("treats any event for a missing auction as a gap, and waits before reading again", () => {
    const sync = syncingWith(bid(2));

    const effects = sync.onLiveStateRead(missing);

    expect(sync.status).toBe("syncing");
    expect(versionsOf(sync.bufferedEvents)).toEqual([2]);
    expect(effects).toEqual([{ kind: "start-sync", reason: "gap", afterBackoff: true }]);
  });
});

describe("onBusEvent while live", () => {
  it("applies the next version, sends it, and updates the state", () => {
    const sync = liveAt(1);
    const next = bid(2);

    const effects = sync.onBusEvent(next);

    expect(sync.status).toBe("live");
    expect(sync.version).toBe(2n);
    expect(sync.known).toMatchObject({
      state: { auctionVersion: "2", leadingBid: { bidId: "1002", amountMinor: "200" } },
    });
    expect(effects).toEqual([
      { kind: "count", counter: "eventsApplied" },
      { kind: "send-bid", bid: bidAcceptedEventOf(next) },
    ]);
  });

  it("drops a duplicate or late copy without changing anything", () => {
    const sync = liveAt(5);
    const known = sync.known;

    for (const version of [5, 4, 1]) {
      const effects = sync.onBusEvent(bid(version));
      expect(sync.known).toBe(known);
      expect(sync.version).toBe(5n);
      expect(sync.status).toBe("live");
      expect(effects).toEqual([{ kind: "count", counter: "eventsDropped" }]);
    }
  });

  it("resyncs at once on a gap, keeping the event that revealed it", () => {
    const sync = liveAt(1);

    const effects = sync.onBusEvent(bid(3));

    expect(sync.status).toBe("syncing");
    expect(sync.version).toBe(1n);
    expect(versionsOf(sync.bufferedEvents)).toEqual([3]);
    expect(effects).toEqual([
      { kind: "start-sync", reason: "gap", afterBackoff: false, receivedVersion: 3n },
    ]);
  });

  it("treats an event type it cannot apply as a gap when it is the next version", () => {
    const sync = liveAt(1);

    const effects = sync.onBusEvent(unsupported(2));

    expect(sync.status).toBe("syncing");
    expect(kinds(effects)).toEqual(["start-sync"]);
  });

  it("still drops an event type it cannot apply when the version is already reflected", () => {
    const sync = liveAt(3);

    const effects = sync.onBusEvent(unsupported(3));

    expect(sync.status).toBe("live");
    expect(counts(effects)).toEqual(["eventsDropped"]);
  });

  it("treats an event for a missing auction as a gap", () => {
    const sync = syncingWith();
    sync.onLiveStateRead(missing);

    const effects = sync.onBusEvent(bid(2));

    expect(sync.status).toBe("syncing");
    expect(effects[0]).toMatchObject({ kind: "start-sync", reason: "gap" });
  });
});

describe("requestResync", () => {
  it("makes a live auction syncing and reads at once", () => {
    const sync = liveAt(4);

    const effects = sync.requestResync("bus-interrupted");

    expect(sync.status).toBe("syncing");
    expect(sync.version).toBe(4n);
    expect(effects).toEqual([
      { kind: "start-sync", reason: "bus-interrupted", afterBackoff: false },
    ]);
  });

  it("keeps the buffer of an auction that was already syncing", () => {
    const sync = syncingWith(bid(2));

    const effects = sync.requestResync("subscription-lost");

    expect(versionsOf(sync.bufferedEvents)).toEqual([2]);
    expect(effects).toEqual([
      { kind: "start-sync", reason: "subscription-lost", afterBackoff: false },
    ]);
  });

  it("waits before reading again only when an attempt that did not settle is repeated", () => {
    expect(syncingWith().requestResync("gap")).toEqual([
      { kind: "start-sync", reason: "gap", afterBackoff: true },
    ]);
    expect(liveAt(1).requestResync("gap")).toEqual([
      { kind: "start-sync", reason: "gap", afterBackoff: false },
    ]);
  });
});

function bidAcceptedEventOf(event: BusAuctionEvent) {
  if (event.kind !== "bid-accepted") throw new Error("expected a bid-accepted event");
  return event.event;
}
