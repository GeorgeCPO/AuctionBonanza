import { describe, expect, it } from "vitest";

import { LiveAuctionBook } from "../../src/live/live-auction-book.js";
import { bidAcceptedData, snapshotData } from "../support/live-fixtures.js";
import type {
  BidAcceptedData,
  LiveEvent,
  SnapshotData,
} from "../../src/live/live-auction-book.types.js";

function snapshot(overrides: Partial<SnapshotData> = {}): LiveEvent {
  return { kind: "snapshot", data: snapshotData(overrides) };
}

function bid(overrides: Partial<BidAcceptedData> = {}): LiveEvent {
  return { kind: "bid-accepted", data: bidAcceptedData(overrides) };
}

function leadingAmount(book: LiveAuctionBook, auctionId = "1"): string | undefined {
  const entry = book.entriesSnapshot().get(auctionId);
  return entry?.kind === "live" ? entry.auction.leadingBid?.amountMinor : undefined;
}

describe("LiveAuctionBook", () => {
  it("applies a bid with a greater version", () => {
    const book = new LiveAuctionBook();
    book.apply(snapshot());

    expect(book.apply(bid())).toBe("applied");
    expect(leadingAmount(book)).toBe("1100");
  });

  it("ignores a duplicate or reordered bid", () => {
    const book = new LiveAuctionBook();
    book.apply(snapshot());
    book.apply(bid());

    expect(book.apply(bid())).toBe("stale");
    expect(book.apply(bid({ auctionVersion: "5", amountMinor: "900" }))).toBe("stale");
    expect(leadingAmount(book)).toBe("1100");
  });

  it("compares versions numerically, not as text or as numbers", () => {
    const book = new LiveAuctionBook();
    book.apply(snapshot({ auctionVersion: "9" }));
    expect(book.apply(bid({ auctionVersion: "10" }))).toBe("applied");

    book.apply(snapshot({ auctionVersion: "9007199254740993" }));
    expect(book.apply(bid({ auctionVersion: "9007199254740992" }))).toBe("stale");
    expect(book.apply(bid({ auctionVersion: "9007199254740994" }))).toBe("applied");
  });

  it("lets a snapshot replace newer state, as after a resync", () => {
    const book = new LiveAuctionBook();
    book.apply(snapshot());
    book.apply(bid());

    const outcome = book.apply(snapshot({ auctionVersion: "5", leadingBid: null }));

    expect(outcome).toBe("replaced");
    expect(leadingAmount(book)).toBeUndefined();
  });

  it("ignores a bid for an auction it holds no state for", () => {
    const book = new LiveAuctionBook();

    expect(book.apply(bid())).toBe("ignored");
  });

  it("ignores a bid for an unavailable auction until a snapshot arrives", () => {
    const book = new LiveAuctionBook();
    book.apply({ kind: "unavailable", data: { auctionId: "1", reason: "not-found" } });

    expect(book.entriesSnapshot().get("1")).toEqual({ kind: "unavailable" });
    expect(book.apply(bid())).toBe("ignored");
  });

  it("keeps closed state from a snapshot", () => {
    const book = new LiveAuctionBook();
    book.apply(snapshot({ phase: "closed", closedAt: "2026-01-02T00:00:05.000Z" }));

    const entry = book.entriesSnapshot().get("1");
    expect(entry?.kind === "live" && entry.auction.closedAt).toBe("2026-01-02T00:00:05.000Z");
  });

  it("forgets auctions that are no longer watched", () => {
    const book = new LiveAuctionBook();
    book.apply(snapshot());
    book.apply(snapshot({ auctionId: "2" }));

    book.retain(new Set(["2"]));

    expect([...book.entriesSnapshot().keys()]).toEqual(["2"]);
  });

  it("hands out copies that later events do not change", () => {
    const book = new LiveAuctionBook();
    book.apply(snapshot());
    const before = book.entriesSnapshot();

    book.apply(bid());

    const entry = before.get("1");
    expect(entry?.kind === "live" && entry.auction.leadingBid?.amountMinor).toBe("1000");
  });
});
