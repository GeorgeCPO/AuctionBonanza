import { BID_ACCEPTED_V1_EVENT_TYPE, parseBidAcceptedV1 } from "@auction-bonanza/contracts";

import { auctionChannel, serializeAuctionEvent } from "../bus/bus.js";

// A consumed auction.events record, reduced to what the relay inspects.
export interface ConsumedAuctionEvent {
  key: string | null;
  value: string | null;
  eventType: string | undefined;
}

export interface PublishDecision {
  kind: "publish";
  channel: string;
  message: string;
}

export interface SkipDecision {
  kind: "skip";
  reason: "unknown-event-type" | "malformed";
  detail: string;
}

export type RelayDecision = PublishDecision | SkipDecision;

// The fields the relay reads from a validated event. The whole event is
// forwarded, not just these.
interface AuctionEvent {
  eventType: string;
  auctionId: string;
}

// Every event type the relay forwards, with the contract parser that
// validates it. An event type missing here is skipped, so a new type must be
// added to the relay before producers emit it.
const eventParsers: Readonly<Record<string, (value: unknown) => AuctionEvent>> = {
  [BID_ACCEPTED_V1_EVENT_TYPE]: parseBidAcceptedV1,
};

// Decides what the relay does with one record. Pure, so every rejection rule
// is testable without Kafka or Valkey.
//
// Anything that cannot be forwarded is skipped rather than retried: the relay
// only feeds live displays, and a skipped version shows up at the edges as a
// gap that they repair from auction-service. Retrying forever would instead
// freeze every auction on the partition.
export function decideRelay(record: ConsumedAuctionEvent): RelayDecision {
  if (record.eventType === undefined) {
    return { kind: "skip", reason: "malformed", detail: "missing event-type header" };
  }
  const parse = eventParsers[record.eventType];
  if (parse === undefined) {
    return {
      kind: "skip",
      reason: "unknown-event-type",
      detail: `no relay contract for ${record.eventType}`,
    };
  }
  if (record.value === null) {
    return { kind: "skip", reason: "malformed", detail: "missing value" };
  }

  let event: AuctionEvent;
  try {
    event = parse(JSON.parse(record.value));
  } catch (error) {
    return {
      kind: "skip",
      reason: "malformed",
      detail: error instanceof Error ? error.message : String(error),
    };
  }
  // The key decides the partition, and so the order the edges rely on. An
  // event keyed by another auction may be ordered against the wrong stream.
  if (record.key !== event.auctionId) {
    return {
      kind: "skip",
      reason: "malformed",
      detail: `key ${String(record.key)} does not match auctionId ${event.auctionId}`,
    };
  }

  return {
    kind: "publish",
    channel: auctionChannel(event.auctionId),
    message: serializeAuctionEvent(event.eventType, event),
  };
}
