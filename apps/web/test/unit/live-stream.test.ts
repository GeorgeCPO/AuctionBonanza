import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { LiveStream } from "../../src/live/live-stream.js";
import type { EventSourceLike } from "../../src/live/live-stream.types.js";

class FakeEventSource implements EventSourceLike {
  onopen: ((event: Event) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  closed = false;
  private readonly listeners = new Map<string, (event: MessageEvent<string>) => void>();

  constructor(readonly url: string) {}

  addEventListener(name: string, listener: (event: MessageEvent<string>) => void): void {
    this.listeners.set(name, listener);
  }

  close(): void {
    this.closed = true;
  }

  open(): void {
    this.onopen?.(new Event("open"));
  }

  fail(): void {
    this.onerror?.(new Event("error"));
  }

  emit(name: string, data: string): void {
    this.listeners.get(name)?.({ data } as MessageEvent<string>);
  }
}

function snapshotJson(auctionId: string, version: string): string {
  return JSON.stringify({
    auctionId,
    auctionVersion: version,
    phase: "open",
    currencyCode: "USD",
    startsAt: "2026-01-01T00:00:00.000Z",
    endsAt: "2026-01-02T00:00:00.000Z",
    cancelledAt: null,
    closedAt: null,
    leadingBid: null,
    serverTime: "2026-01-01T12:00:00.000Z",
  });
}

function setup() {
  const sources: FakeEventSource[] = [];
  const stream = new LiveStream({
    createEventSource: (url) => {
      const source = new FakeEventSource(url);
      sources.push(source);
      return source;
    },
    now: () => Date.parse("2026-01-01T12:00:05.000Z"),
  });
  return { stream, sources };
}

describe("LiveStream", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("opens one stream for the sorted, de-duplicated ids", () => {
    const { stream, sources } = setup();

    stream.watch(["3", "1", "3"]);

    expect(sources).toHaveLength(1);
    expect(sources[0]?.url).toBe("/gateway/v1/stream?auctions=1,3");
    expect(stream.getView().status).toBe("connecting");
  });

  it("does nothing when the same set is watched again", () => {
    const { stream, sources } = setup();
    stream.watch(["1", "2"]);

    stream.watch(["2", "1"]);

    expect(sources).toHaveLength(1);
    expect(sources[0]?.closed).toBe(false);
  });

  it("reconnects for a changed set and drops state of unwatched auctions", () => {
    const { stream, sources } = setup();
    stream.watch(["1", "2"]);
    sources[0]?.emit("snapshot", snapshotJson("1", "1"));
    sources[0]?.emit("snapshot", snapshotJson("2", "1"));

    stream.watch(["2"]);

    expect(sources[0]?.closed).toBe(true);
    expect(sources).toHaveLength(2);
    expect([...stream.getView().entries.keys()]).toEqual(["2"]);
  });

  it("goes idle and closes the stream when the set becomes empty", () => {
    const { stream, sources } = setup();
    stream.watch(["1"]);

    stream.watch([]);

    expect(sources[0]?.closed).toBe(true);
    expect(stream.getView().status).toBe("idle");
    expect(sources).toHaveLength(1);
  });

  it("applies frames, logs them, and takes the clock offset from a snapshot", () => {
    const { stream, sources } = setup();
    stream.watch(["1"]);
    sources[0]?.open();

    sources[0]?.emit("snapshot", snapshotJson("1", "1"));
    sources[0]?.emit(
      "bid-accepted",
      JSON.stringify({
        auctionId: "1",
        auctionVersion: "2",
        bidId: "9",
        amountMinor: "1100",
        currencyCode: "USD",
        occurredAt: "2026-01-01T12:00:01.000Z",
      }),
    );

    const view = stream.getView();
    expect(view.status).toBe("open");
    expect(view.clockOffsetMs).toBe(-5000);
    const entry = view.entries.get("1");
    expect(entry?.kind === "live" && entry.auction.leadingBid?.amountMinor).toBe("1100");
    expect(view.log.map((line) => line.text)).toEqual([
      "bid-accepted v2 1100 (applied)",
      "snapshot v1 open",
    ]);
  });

  it.each(["not json", "null", "42", "{}", '{"auctionId":1}'])(
    "logs an unreadable frame and keeps going: %s",
    (raw) => {
      const { stream, sources } = setup();
      stream.watch(["1"]);

      expect(() => sources[0]?.emit("snapshot", raw)).not.toThrow();
      sources[0]?.emit("snapshot", snapshotJson("1", "1"));

      expect(stream.getView().log.map((line) => line.text)).toEqual([
        "snapshot v1 open",
        "unreadable snapshot frame",
      ]);
      expect(stream.getView().entries.has("1")).toBe(true);
    },
  );

  it("reconnects with a doubling delay that is capped, and resets once open", () => {
    const { stream, sources } = setup();
    stream.watch(["1"]);

    const delays: number[] = [];
    for (let attempt = 0; attempt < 6; attempt += 1) {
      const source = sources.at(-1);
      source?.fail();
      expect(source?.closed).toBe(true);
      expect(stream.getView().status).toBe("reconnecting");
      const before = sources.length;
      let waited = 0;
      while (sources.length === before) {
        vi.advanceTimersByTime(1000);
        waited += 1000;
      }
      delays.push(waited);
    }
    expect(delays).toEqual([1000, 2000, 4000, 8000, 15000, 15000]);

    sources.at(-1)?.open();
    sources.at(-1)?.fail();
    const before = sources.length;
    vi.advanceTimersByTime(1000);
    expect(sources).toHaveLength(before + 1);
  });

  it("resets the delay when a new set is watched after failures", () => {
    const { stream, sources } = setup();
    stream.watch(["1"]);
    for (let attempt = 0; attempt < 3; attempt += 1) {
      sources.at(-1)?.fail();
      vi.advanceTimersByTime(15_000);
    }

    stream.watch(["2"]);
    sources.at(-1)?.fail();
    const before = sources.length;
    vi.advanceTimersByTime(1000);

    expect(sources).toHaveLength(before + 1);
  });

  it("cancels a pending reconnect when the set changes or the stream is closed", () => {
    const { stream, sources } = setup();
    stream.watch(["1"]);
    sources[0]?.fail();

    stream.watch(["2"]);
    expect(sources).toHaveLength(2);
    vi.advanceTimersByTime(60_000);
    expect(sources).toHaveLength(2);

    sources[1]?.fail();
    stream.close();
    vi.advanceTimersByTime(60_000);
    expect(sources).toHaveLength(2);
    expect(stream.getView().status).toBe("idle");
  });

  it("notifies subscribers until they unsubscribe", () => {
    const { stream } = setup();
    const listener = vi.fn<() => void>();
    const unsubscribe = stream.subscribe(listener);

    stream.watch(["1"]);
    const calls = listener.mock.calls.length;
    unsubscribe();
    stream.watch(["2"]);

    expect(calls).toBeGreaterThan(0);
    expect(listener).toHaveBeenCalledTimes(calls);
  });
});
