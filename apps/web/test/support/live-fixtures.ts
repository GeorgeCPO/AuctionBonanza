import type { BidAcceptedData, SnapshotData } from "../../src/live/live-auction-book.types.js";

// Gateway frame payloads, shared so the book and stream tests agree on the wire shape.
export function snapshotData(overrides: Partial<SnapshotData> = {}): SnapshotData {
  return {
    auctionId: "1",
    auctionVersion: "5",
    phase: "open",
    currencyCode: "USD",
    startsAt: "2026-01-01T00:00:00.000Z",
    endsAt: "2026-01-02T00:00:00.000Z",
    cancelledAt: null,
    closedAt: null,
    leadingBid: { bidId: "10", amountMinor: "1000" },
    serverTime: "2026-01-01T12:00:00.000Z",
    ...overrides,
  };
}

export function bidAcceptedData(overrides: Partial<BidAcceptedData> = {}): BidAcceptedData {
  return {
    auctionId: "1",
    auctionVersion: "6",
    bidId: "11",
    amountMinor: "1100",
    currencyCode: "USD",
    occurredAt: "2026-01-01T12:00:01.000Z",
    ...overrides,
  };
}
