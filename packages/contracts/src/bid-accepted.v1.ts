import { type Static, Type } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

export const BID_ACCEPTED_V1_EVENT_TYPE = "BidAccepted.v1" as const;

const positiveDecimalString = Type.String({ pattern: "^[1-9][0-9]*$" });
const occurredAtPattern =
  "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?Z$";

export const BidAcceptedV1Schema = Type.Object(
  {
    eventId: Type.String({
      pattern: "^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    }),
    eventType: Type.Literal(BID_ACCEPTED_V1_EVENT_TYPE),
    occurredAt: Type.String({ pattern: occurredAtPattern }),
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

function isValidOccurredAt(value: string): boolean {
  const year = Number(value.slice(0, 4));
  const month = Number(value.slice(5, 7));
  const day = Number(value.slice(8, 10));
  const hour = Number(value.slice(11, 13));
  const minute = Number(value.slice(14, 16));
  const second = Number(value.slice(17, 19));

  if (hour > 23 || minute > 59 || second > 59) return false;

  const daysInMonth =
    month === 2
      ? isLeapYear(year)
        ? 29
        : 28
      : month === 4 || month === 6 || month === 9 || month === 11
        ? 30
        : month >= 1 && month <= 12
          ? 31
          : 0;
  return day >= 1 && day <= daysInMonth;
}

function isLeapYear(year: number): boolean {
  return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
}
