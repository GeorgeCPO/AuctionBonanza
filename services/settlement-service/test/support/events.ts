import { randomUUID } from "node:crypto";

import type { AuctionClosedV1 } from "@auction-bonanza/contracts";

import type { ConsumedRecord } from "../../src/consumed-record.types.js";

// A sold AuctionClosed.v1 with made-up ids. Tests that need ids that exist in
// a database build their own.
export function soldEvent(overrides: Partial<AuctionClosedV1> = {}): AuctionClosedV1 {
  return {
    eventId: randomUUID(),
    eventType: "AuctionClosed.v1",
    occurredAt: "2030-01-01T10:00:00.000000Z",
    auctionId: "42",
    auctionVersion: "8",
    currencyCode: "USD",
    winningBidId: "91",
    winnerId: "12",
    finalAmountMinor: "12500",
    ...overrides,
  };
}

export function unsoldEvent(): AuctionClosedV1 {
  return soldEvent({ winningBidId: null, winnerId: null, finalAmountMinor: null });
}

// A BidAccepted.v1 record's payload. Settlement never reads it, only skips it.
export function bidAcceptedPayload(auctionId: string, version: number): object {
  return {
    eventId: randomUUID(),
    eventType: "BidAccepted.v1",
    occurredAt: "2030-01-01T10:00:00.000000Z",
    auctionId,
    auctionVersion: String(version),
    bidId: String(1000 + version),
    amountMinor: String(100 * version),
    currencyCode: "USD",
  };
}

export interface RecordOptions {
  // Defaults to the event's auctionId, as the outbox publisher sets it.
  key?: string | null;
  // Defaults to the event's type and id. A header set to undefined is absent.
  eventType?: string | undefined;
  eventId?: string | undefined;
  value?: string | null;
  partition?: number;
  offset?: string;
}

// A consumed record shaped like the outbox publisher's messages: key
// auctionId, headers event-type and event-id, and the event as JSON.
export function recordOf(event: AuctionClosedV1, options: RecordOptions = {}): ConsumedRecord {
  const key = "key" in options ? options.key : event.auctionId;
  const eventType = "eventType" in options ? options.eventType : event.eventType;
  const eventId = "eventId" in options ? options.eventId : event.eventId;
  const value = "value" in options ? options.value : JSON.stringify(event);
  const headers: Record<string, Buffer> = {};
  if (eventType !== undefined) {
    headers["event-type"] = Buffer.from(eventType);
  }
  if (eventId !== undefined) {
    headers["event-id"] = Buffer.from(eventId);
  }
  headers["content-type"] = Buffer.from("application/json");

  return {
    topic: "auction.events",
    partition: options.partition ?? 3,
    offset: options.offset ?? "17",
    key: key === null || key === undefined ? null : Buffer.from(key),
    value: value === null || value === undefined ? null : Buffer.from(value),
    headers,
  };
}
