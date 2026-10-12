import { describe, expect, it } from "vitest";

import {
  AUCTION_CLOSED_V1_EVENT_TYPE,
  isAuctionClosedV1,
  parseAuctionClosedV1,
} from "../src/index.js";

const soldEvent = {
  eventId: "bb106586-ded1-4eca-9a75-1a38c170499f",
  eventType: AUCTION_CLOSED_V1_EVENT_TYPE,
  occurredAt: "2026-10-12T09:15:30.123456Z",
  auctionId: "42",
  auctionVersion: "8",
  currencyCode: "USD",
  winningBidId: "91",
  winnerId: "12",
  finalAmountMinor: "12500",
};

const unsoldEvent = { ...soldEvent, winningBidId: null, winnerId: null, finalAmountMinor: null };

describe("AuctionClosed.v1", () => {
  it.each([
    ["sold", soldEvent],
    ["unsold", unsoldEvent],
  ])("accepts a %s auction", (_label, event) => {
    expect(isAuctionClosedV1(event)).toBe(true);
    expect(parseAuctionClosedV1(event)).toBe(event);
  });

  it.each([
    ["a winner without an amount", { ...soldEvent, finalAmountMinor: null }],
    ["an amount without a winner", { ...unsoldEvent, finalAmountMinor: "100" }],
    ["a bid without a bidder", { ...soldEvent, winnerId: null }],
    ["numeric IDs", { ...soldEvent, winningBidId: 91 }],
    ["a zero amount", { ...soldEvent, finalAmountMinor: "0" }],
    ["the wrong event version", { ...soldEvent, eventType: "AuctionClosed.v2" }],
    ["additional fields", { ...soldEvent, closedAt: "2026-10-12T09:15:30Z" }],
    ["missing fields", { ...soldEvent, winnerId: undefined }],
    ["an impossible calendar day", { ...soldEvent, occurredAt: "2026-02-29T09:15:30Z" }],
  ])("rejects %s", (_label, candidate) => {
    expect(isAuctionClosedV1(candidate)).toBe(false);
    expect(() => parseAuctionClosedV1(candidate)).toThrow(TypeError);
  });
});
