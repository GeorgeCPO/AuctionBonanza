import {
  AUCTION_CLOSED_V1_EVENT_TYPE,
  AuctionClosedV1Schema,
  isAuctionClosedV1,
} from "@auction-bonanza/contracts";
import { Value } from "@sinclair/typebox/value";

import type {
  DecisionInput,
  RejectDecision,
  RejectionReason,
  SettlementDecision,
} from "./settlement.decision.types.js";

// Decides what settlement does with one record. Pure, so every rule is
// testable without Kafka or PostgreSQL.
//
// The two ways out of "settle" differ on purpose. A record of another event
// type is ignored: settlement does not own it, and ignoring it loses nothing
// settlement needs. A record that claims to be an AuctionClosed.v1 but cannot
// be read as one is rejected, not skipped: it may be a winner, so it must reach
// a person instead of vanishing. This is the opposite of the realtime relay,
// whose display path can skip a bad record because the edges repair the gap
// from auction-service.
export function decideSettlement(record: DecisionInput): SettlementDecision {
  if (record.eventType === undefined) {
    return reject("missing-event-type", "missing event-type header");
  }
  if (record.eventType !== AUCTION_CLOSED_V1_EVENT_TYPE) {
    return { kind: "ignore", eventType: record.eventType };
  }
  if (record.value === null) {
    return reject("missing-value", "missing value");
  }

  let payload: unknown;
  try {
    payload = JSON.parse(record.value);
  } catch (error) {
    return reject("invalid-json", error instanceof Error ? error.message : String(error));
  }
  if (!isAuctionClosedV1(payload)) {
    return reject("contract-violation", describeContractViolation(payload));
  }

  // The key decides the partition, and so which events of an auction are
  // ordered against each other. An event keyed by another auction was not
  // produced by the outbox publisher.
  if (record.key !== payload.auctionId) {
    return reject(
      "key-mismatch",
      `key ${String(record.key)} does not match auctionId ${payload.auctionId}`,
    );
  }
  // The header is the de-duplication key the publisher promises. A record
  // whose header and payload disagree cannot be trusted to be either.
  if (record.eventId !== payload.eventId) {
    return reject(
      "event-id-mismatch",
      `event-id header ${String(record.eventId)} does not match eventId ${payload.eventId}`,
    );
  }

  return { kind: "settle", event: payload };
}

function reject(reason: RejectionReason, detail: string): RejectDecision {
  return { kind: "reject", reason, detail };
}

// The schema's first complaint when there is one. A payload the schema accepts
// but the contract does not (an invalid timestamp, or half an outcome) has no
// schema error to quote.
function describeContractViolation(payload: unknown): string {
  const first = Value.Errors(AuctionClosedV1Schema, payload).First();
  if (first === undefined) {
    return "payload satisfies the schema but not the AuctionClosed.v1 contract";
  }
  return `${first.path === "" ? "/" : first.path}: ${first.message}`;
}
