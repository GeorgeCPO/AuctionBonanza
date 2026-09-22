import { describe, expect, it } from "vitest";

import { BID_ACCEPTED_V1_EVENT_TYPE, isBidAcceptedV1, parseBidAcceptedV1 } from "../src/index.js";

const validEvent = {
  eventId: "bb106586-ded1-4eca-9a75-1a38c170499f",
  eventType: BID_ACCEPTED_V1_EVENT_TYPE,
  occurredAt: "2026-09-22T09:15:30.123456Z",
  auctionId: "42",
  auctionVersion: "7",
  bidId: "91",
  amountMinor: "12500",
  currencyCode: "USD",
};

describe("BidAccepted.v1", () => {
  it("accepts the versioned event shape", () => {
    expect(isBidAcceptedV1(validEvent)).toBe(true);
    expect(parseBidAcceptedV1(validEvent)).toBe(validEvent);
  });

  it.each([
    ["numeric IDs", { ...validEvent, bidId: 91 }],
    ["numeric versions", { ...validEvent, auctionVersion: 7 }],
    ["numeric amounts", { ...validEvent, amountMinor: 12500 }],
    ["the wrong event version", { ...validEvent, eventType: "BidAccepted.v2" }],
    ["additional fields", { ...validEvent, bidderId: "12" }],
    ["an impossible month", { ...validEvent, occurredAt: "2026-13-22T09:15:30Z" }],
    ["an impossible calendar day", { ...validEvent, occurredAt: "2026-02-29T09:15:30Z" }],
    ["an impossible time", { ...validEvent, occurredAt: "2026-09-22T24:00:00Z" }],
  ])("rejects %s", (_label, candidate) => {
    expect(isBidAcceptedV1(candidate)).toBe(false);
    expect(() => parseBidAcceptedV1(candidate)).toThrow(TypeError);
  });

  it("accepts February 29 in a leap year", () => {
    expect(isBidAcceptedV1({ ...validEvent, occurredAt: "2028-02-29T23:59:59.999999Z" })).toBe(
      true,
    );
  });
});
