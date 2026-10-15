import { describe, expect, it } from "vitest";

import type { AuctionJson } from "../../src/api/api.types.js";
import type { LiveEntry } from "../../src/live/live-auction-book.types.js";
import { viewAuction } from "../../src/ui/auction-view.js";

const row: AuctionJson = {
  id: "1",
  ownerId: "9",
  title: "t",
  description: "d",
  currencyCode: "USD",
  startsAt: "2026-01-01T00:00:00.000Z",
  endsAt: "2026-01-01T01:00:00.000Z",
  startingPriceMinor: "1000",
  minimumBidIncrementMinor: "100",
  version: "1",
  cancelledAt: null,
  closedAt: null,
  winningBidId: null,
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
  phase: "open",
};

function liveEntry(
  overrides: Partial<Extract<LiveEntry, { kind: "live" }>["auction"]> = {},
): LiveEntry {
  return {
    kind: "live",
    auction: {
      version: "3",
      currencyCode: "USD",
      startsAt: row.startsAt,
      endsAt: row.endsAt,
      cancelledAt: null,
      closedAt: null,
      leadingBid: { bidId: "5", amountMinor: "1500" },
      ...overrides,
    },
  };
}

const during = Date.parse("2026-01-01T00:30:00.000Z");
const after = Date.parse("2026-01-01T02:00:00.000Z");

describe("viewAuction", () => {
  it("shows the starting price from the fetched row when the stream has nothing", () => {
    expect(viewAuction(row, undefined, during)).toEqual({
      phase: "open",
      priceMinor: "1000",
      hasBid: false,
      currencyCode: "USD",
      countdown: "30m 00s",
      version: undefined,
    });
  });

  it("falls back to the row for an unavailable auction", () => {
    expect(viewAuction(row, { kind: "unavailable" }, during).version).toBeUndefined();
  });

  it("shows the leading bid and version from the live state", () => {
    const view = viewAuction(row, liveEntry(), during);

    expect(view).toMatchObject({ priceMinor: "1500", hasBid: true, version: "3" });
  });

  it("shows a live state without a bid at the starting price", () => {
    const view = viewAuction(row, liveEntry({ leadingBid: null }), during);

    expect(view).toMatchObject({ priceMinor: "1000", hasBid: false, version: "3" });
  });

  it("has no countdown once the auction is not open", () => {
    expect(viewAuction(row, liveEntry(), after)).toMatchObject({
      phase: "ended",
      countdown: undefined,
    });
  });

  it("takes closedAt or cancelledAt from the fetched row when the snapshot predates them", () => {
    const closed = { ...row, closedAt: "2026-01-01T01:00:05.000Z" };
    const cancelled = { ...row, cancelledAt: "2026-01-01T00:10:00.000Z" };

    expect(viewAuction(closed, liveEntry(), after).phase).toBe("closed");
    expect(viewAuction(cancelled, liveEntry(), during).phase).toBe("cancelled");
  });
});
