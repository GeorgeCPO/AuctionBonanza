import { type Static, Type } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

import {
  EVENT_ID_PATTERN,
  isValidOccurredAt,
  OCCURRED_AT_PATTERN,
  POSITIVE_DECIMAL_PATTERN,
} from "./event-fields.js";

export const BID_ACCEPTED_V1_EVENT_TYPE = "BidAccepted.v1" as const;

const positiveDecimalString = Type.String({ pattern: POSITIVE_DECIMAL_PATTERN });

export const BidAcceptedV1Schema = Type.Object(
  {
    eventId: Type.String({ pattern: EVENT_ID_PATTERN }),
    eventType: Type.Literal(BID_ACCEPTED_V1_EVENT_TYPE),
    occurredAt: Type.String({ pattern: OCCURRED_AT_PATTERN }),
    auctionId: positiveDecimalString,
    auctionVersion: positiveDecimalString,
    bidId: positiveDecimalString,
    amountMinor: positiveDecimalString,
    currencyCode: Type.String({ pattern: "^[A-Z]{3}$" }),
  },
  { additionalProperties: false },
);

export type BidAcceptedV1 = Static<typeof BidAcceptedV1Schema>;

export function isBidAcceptedV1(value: unknown): value is BidAcceptedV1 {
  return Value.Check(BidAcceptedV1Schema, value) && isValidOccurredAt(value.occurredAt);
}

export function parseBidAcceptedV1(value: unknown): BidAcceptedV1 {
  if (!isBidAcceptedV1(value)) {
    throw new TypeError("Value does not satisfy the BidAccepted.v1 contract");
  }
  return value;
}
