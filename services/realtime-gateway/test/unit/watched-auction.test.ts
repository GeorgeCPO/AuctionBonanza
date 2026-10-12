import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { AuctionWatcher } from "../../src/edge/auction-registry.types.js";
import { WatchedAuction } from "../../src/edge/watched-auction.js";

// WatchedAuction holds a watched auction's clients and timers. Time is fake,
// so timers fire exactly when the test advances the clock.

class RecordingWatcher implements AuctionWatcher {
  readonly frames: string[] = [];
  send(frame: string): void {
    this.frames.push(frame);
  }
}

function auction(): WatchedAuction {
  return new WatchedAuction("42", 100);
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("a new WatchedAuction", () => {
  it("knows its channel and starts syncing", () => {
    const watched = auction();

    expect(watched.auctionId).toBe("42");
    expect(watched.channel).toBe("auction:42");
    expect(watched.sync.status).toBe("syncing");
    expect(watched.isDiscarded).toBe(false);
  });
});

describe("watchers", () => {
  it("sends a frame to every watcher, once each", () => {
    const watched = auction();
    const first = new RecordingWatcher();
    const second = new RecordingWatcher();
    watched.addWatcher(first);
    watched.addWatcher(second);
    watched.addWatcher(first);

    watched.sendToWatchers("frame");

    expect(first.frames).toEqual(["frame"]);
    expect(second.frames).toEqual(["frame"]);
  });

  it("reports the removal of the last watcher, and only that", () => {
    const watched = auction();
    const first = new RecordingWatcher();
    const second = new RecordingWatcher();
    watched.addWatcher(first);
    watched.addWatcher(second);

    expect(watched.removeWatcher(new RecordingWatcher())).toBe(false);
    expect(watched.removeWatcher(first)).toBe(false);
    expect(watched.removeWatcher(first)).toBe(false);
    expect(watched.removeWatcher(second)).toBe(true);
  });

  it("does not report a last watcher leaving when nobody was there to leave", () => {
    const watched = auction();
    const only = new RecordingWatcher();
    watched.addWatcher(only);

    expect(watched.removeWatcher(only)).toBe(true);
    // A connection that closes unwatches every auction of its stream, also
    // the ones it never reached or already left.
    expect(watched.removeWatcher(only)).toBe(false);
    expect(watched.removeWatcher(new RecordingWatcher())).toBe(false);
    expect(auction().removeWatcher(only)).toBe(false);
  });

  it("lets a watcher leave while a frame is being sent, without skipping the others", () => {
    const watched = auction();
    const received: string[] = [];
    const leaving: AuctionWatcher = {
      send: () => {
        received.push("leaving");
        watched.removeWatcher(leaving);
      },
    };
    const staying: AuctionWatcher = { send: () => received.push("staying") };
    watched.addWatcher(leaving);
    watched.addWatcher(staying);

    watched.sendToWatchers("frame");
    watched.sendToWatchers("frame");

    expect(received).toEqual(["leaving", "staying", "staying"]);
  });
});

describe("linger", () => {
  it("expires after its duration", () => {
    const watched = auction();
    const onExpire = vi.fn<() => void>();

    watched.startLinger(1_000, onExpire);
    vi.advanceTimersByTime(999);
    expect(onExpire).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);

    expect(onExpire).toHaveBeenCalledTimes(1);
  });

  it("is cancelled by a watcher arriving", () => {
    const watched = auction();
    const onExpire = vi.fn<() => void>();
    watched.startLinger(1_000, onExpire);

    vi.advanceTimersByTime(500);
    watched.addWatcher(new RecordingWatcher());
    vi.advanceTimersByTime(5_000);

    expect(onExpire).not.toHaveBeenCalled();
  });

  it("restarts when started again, and expires once", () => {
    const watched = auction();
    const onExpire = vi.fn<() => void>();
    watched.startLinger(1_000, onExpire);

    vi.advanceTimersByTime(800);
    watched.startLinger(1_000, onExpire);
    vi.advanceTimersByTime(800);
    expect(onExpire).not.toHaveBeenCalled();
    vi.advanceTimersByTime(5_000);

    expect(onExpire).toHaveBeenCalledTimes(1);
  });
});

describe("sync attempts", () => {
  it("numbers attempts, and calls an older one stale", () => {
    const watched = auction();

    const first = watched.beginAttempt();
    expect(watched.isStale(first)).toBe(false);
    const second = watched.beginAttempt();

    expect(second).toBeGreaterThan(first);
    expect(watched.isStale(first)).toBe(true);
    expect(watched.isStale(second)).toBe(false);
  });

  it("cancels the retry of an earlier attempt when a new one begins", () => {
    const watched = auction();
    const retry = vi.fn<() => void>();
    watched.beginAttempt();
    watched.scheduleRetry(100, retry);

    watched.beginAttempt();
    vi.advanceTimersByTime(1_000);

    expect(retry).not.toHaveBeenCalled();
  });

  it("runs a retry after its delay, and only the latest one scheduled", () => {
    const watched = auction();
    const early = vi.fn<() => void>();
    const latest = vi.fn<() => void>();

    watched.scheduleRetry(100, early);
    watched.scheduleRetry(200, latest);
    vi.advanceTimersByTime(1_000);

    expect(early).not.toHaveBeenCalled();
    expect(latest).toHaveBeenCalledTimes(1);
  });

  it("counts attempts that did not end live until the count is reset", () => {
    const watched = auction();

    expect([1, 2, 3].map(() => watched.nextBackoffAttempt())).toEqual([1, 2, 3]);
    watched.resetBackoff();

    expect(watched.nextBackoffAttempt()).toBe(1);
  });
});

describe("subscribeOnce", () => {
  it("opens one subscription however often it is asked, sharing the pending one", async () => {
    const watched = auction();
    const confirmed = Promise.withResolvers<void>();
    const open = vi.fn<() => Promise<void>>(() => confirmed.promise);

    const first = watched.subscribeOnce(open);
    const second = watched.subscribeOnce(open);
    confirmed.resolve();
    await Promise.all([first, second]);
    await watched.subscribeOnce(open);

    expect(open).toHaveBeenCalledTimes(1);
  });

  it("forgets a subscription that failed, so the next call tries again", async () => {
    const watched = auction();
    const open = vi
      .fn<() => Promise<void>>()
      .mockRejectedValueOnce(new Error("subscribe failed"))
      .mockResolvedValue(undefined);

    await expect(watched.subscribeOnce(open)).rejects.toThrow("subscribe failed");
    await watched.subscribeOnce(open);

    expect(open).toHaveBeenCalledTimes(2);
  });

  it("opens a new subscription after the bus dropped the old one", async () => {
    const watched = auction();
    const open = vi.fn<() => Promise<void>>(async () => {});
    await watched.subscribeOnce(open);

    watched.forgetSubscription();
    await watched.subscribeOnce(open);

    expect(open).toHaveBeenCalledTimes(2);
  });

  it("keeps a newer subscription when an older, forgotten one fails late", async () => {
    const watched = auction();
    const older = Promise.withResolvers<void>();
    const open = vi
      .fn<() => Promise<void>>()
      .mockReturnValueOnce(older.promise)
      .mockResolvedValue(undefined);
    const olderResult = watched.subscribeOnce(open);
    watched.forgetSubscription();
    await watched.subscribeOnce(open);

    older.reject(new Error("too late"));
    await expect(olderResult).rejects.toThrow("too late");
    await watched.subscribeOnce(open);

    expect(open).toHaveBeenCalledTimes(2);
  });
});

describe("discard", () => {
  it("marks the auction gone, makes every attempt stale, and stops both timers", () => {
    const watched = auction();
    const attempt = watched.beginAttempt();
    const retry = vi.fn<() => void>();
    const onExpire = vi.fn<() => void>();
    watched.scheduleRetry(100, retry);
    watched.startLinger(100, onExpire);

    watched.discard();
    vi.advanceTimersByTime(1_000);

    expect(watched.isDiscarded).toBe(true);
    expect(watched.isStale(attempt)).toBe(true);
    expect(retry).not.toHaveBeenCalled();
    expect(onExpire).not.toHaveBeenCalled();
  });
});
