import { type Static, Type } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

import {
  EVENT_ID_PATTERN,
  isValidOccurredAt,
  OCCURRED_AT_PATTERN,
  POSITIVE_DECIMAL_PATTERN,
} from "./event-fields.js";

export const AUCTION_CLOSED_V1_EVENT_TYPE = "AuctionClosed.v1" as const;

const positiveDecimalString = Type.String({ pattern: POSITIVE_DECIMAL_PATTERN });

// The outcome of finalization. A sold auction names the winning bid, its
// bidder, and the price; an unsold one carries null in all three. The schema
// below accepts exactly those two shapes.
export const AuctionClosedV1Schema = Type.Object(
  {
    eventId: Type.String({ pattern: EVENT_ID_PATTERN }),
    eventType: Type.Literal(AUCTION_CLOSED_V1_EVENT_TYPE),
    occurredAt: Type.String({ pattern: OCCURRED_AT_PATTERN }),
    auctionId: positiveDecimalString,
    auctionVersion: positiveDecimalString,
    currencyCode: Type.String({ pattern: "^[A-Z]{3}$" }),
    winningBidId: Type.Union([positiveDecimalString, Type.Null()]),
    winnerId: Type.Union([positiveDecimalString, Type.Null()]),
    finalAmountMinor: Type.Union([positiveDecimalString, Type.Null()]),
  },
  { additionalProperties: false },
);

export type AuctionClosedV1 = Static<typeof AuctionClosedV1Schema>;

export function isAuctionClosedV1(value: unknown): value is AuctionClosedV1 {
  return (
    Value.Check(AuctionClosedV1Schema, value) &&
    isValidOccurredAt(value.occurredAt) &&
    hasConsistentOutcome(value)
  );
}

export function parseAuctionClosedV1(value: unknown): AuctionClosedV1 {
  if (!isAuctionClosedV1(value)) {
    throw new TypeError("Value does not satisfy the AuctionClosed.v1 contract");
  }
  return value;
}

// Sold or unsold, never half of each.
function hasConsistentOutcome(event: AuctionClosedV1): boolean {
  const present = [event.winningBidId, event.winnerId, event.finalAmountMinor].filter(
    (field) => field !== null,
  ).length;
  return present === 0 || present === 3;
}
