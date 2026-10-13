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

// The first rule a value breaks, named so a person can act on it, or undefined
// when the value is a valid AuctionClosed.v1. The schema's own complaint comes
// first, then the two rules a schema cannot express: a real timestamp, and an
// outcome that is sold or unsold, never half of each.
export function findAuctionClosedV1Violation(value: unknown): string | undefined {
  if (!Value.Check(AuctionClosedV1Schema, value)) {
    // A value that fails the check always yields at least one error.
    const first = Value.Errors(AuctionClosedV1Schema, value).First()!;
    return `${first.path === "" ? "/" : first.path}: ${first.message}`;
  }
  if (!isValidOccurredAt(value.occurredAt)) {
    return "/occurredAt: is not a real UTC timestamp";
  }
  if (!hasConsistentOutcome(value)) {
    return "/winningBidId, /winnerId and /finalAmountMinor: must be all set or all null";
  }
  return undefined;
}

export function isAuctionClosedV1(value: unknown): value is AuctionClosedV1 {
  return findAuctionClosedV1Violation(value) === undefined;
}

export function parseAuctionClosedV1(value: unknown): AuctionClosedV1 {
  if (!isAuctionClosedV1(value)) {
    const violation = findAuctionClosedV1Violation(value);
    throw new TypeError(`Value does not satisfy the AuctionClosed.v1 contract: ${violation}`);
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
