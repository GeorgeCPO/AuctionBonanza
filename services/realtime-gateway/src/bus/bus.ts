// The realtime gateway's internal pub/sub contract between the relay and the
// edges. It is owned by this service, not by packages/contracts: nothing
// outside the gateway publishes or subscribes to these channels.
//
// See "Bus" in docs/REALTIME_GATEWAY.md.

import { BID_ACCEPTED_V1_EVENT_TYPE, parseBidAcceptedV1 } from "@auction-bonanza/contracts";

import type { AuctionEventEnvelope, BusAuctionEvent } from "./bus.types.js";

export const BUS_ENVELOPE_VERSION = 1;

const AUCTION_CHANNEL_PREFIX = "auction:";

export function auctionChannel(auctionId: string): string {
  return `${AUCTION_CHANNEL_PREFIX}${auctionId}`;
}

// The inverse of auctionChannel: the auction id of a channel, or undefined
// for a channel that does not belong to an auction.
export function auctionIdOfChannel(channel: string): string | undefined {
  return channel.startsWith(AUCTION_CHANNEL_PREFIX)
    ? channel.slice(AUCTION_CHANNEL_PREFIX.length)
    : undefined;
}

export function serializeAuctionEvent(eventType: string, event: unknown): string {
  const envelope: AuctionEventEnvelope = { v: BUS_ENVELOPE_VERSION, eventType, event };
  return JSON.stringify(envelope);
}

// Parses a bus message. Throws when the message is not a valid envelope, or
// when the event does not name its auction and version: such a message
// cannot be placed in an auction's sequence, so the edge can only log it and
// rely on the next event of that auction to reveal the gap.
export function parseAuctionEventEnvelope(message: string): BusAuctionEvent {
  const envelope = JSON.parse(message) as Partial<AuctionEventEnvelope> | null;
  if (typeof envelope !== "object" || envelope === null || envelope.v !== BUS_ENVELOPE_VERSION) {
    throw new TypeError("Message is not a version 1 bus envelope");
  }
  if (typeof envelope.eventType !== "string") {
    throw new TypeError("Bus envelope has no event type");
  }

  if (envelope.eventType === BID_ACCEPTED_V1_EVENT_TYPE) {
    const event = parseBidAcceptedV1(envelope.event);
    return {
      kind: "bid-accepted",
      auctionId: event.auctionId,
      version: BigInt(event.auctionVersion),
      event,
    };
  }

  // A type this edge does not know yet, for example one the relay learned
  // first. It still changed the auction, so the edge must resync on it.
  const { auctionId, auctionVersion } = (envelope.event ?? {}) as Record<string, unknown>;
  if (
    typeof auctionId !== "string" ||
    !decimalIdPattern.test(auctionId) ||
    typeof auctionVersion !== "string" ||
    !decimalIdPattern.test(auctionVersion)
  ) {
    throw new TypeError(`${envelope.eventType} event has no auctionId and auctionVersion`);
  }
  return {
    kind: "unsupported",
    auctionId,
    version: BigInt(auctionVersion),
    eventType: envelope.eventType,
  };
}

const decimalIdPattern = /^[1-9][0-9]*$/;
