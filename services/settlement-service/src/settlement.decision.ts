import {
  AUCTION_CLOSED_V1_EVENT_TYPE,
  BID_ACCEPTED_V1_EVENT_TYPE,
  findAuctionClosedV1Violation,
  isAuctionClosedV1,
} from "@auction-bonanza/contracts";

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
// type is ignored: settlement does not own it. Only BidAccepted.v1 is known to
// be irrelevant, so any other type is flagged as unexpected, because a close
// under a new name would otherwise vanish without a trace. A record that claims
// to be an AuctionClosed.v1 but cannot be read as one is rejected, not skipped:
// it may be a winner, so it must reach a person instead of vanishing. This is
// the opposite of the realtime relay, whose display path can skip a bad record
// because the edges repair the gap from auction-service.
export function decideSettlement(record: DecisionInput): SettlementDecision {
  if (record.eventType === undefined) {
    return reject("missing-event-type", "missing event-type header");
  }
  if (record.eventType !== AUCTION_CLOSED_V1_EVENT_TYPE) {
    return {
      kind: "ignore",
      eventType: record.eventType,
      expected: record.eventType === BID_ACCEPTED_V1_EVENT_TYPE,
    };
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
    const violation = findAuctionClosedV1Violation(payload);
    return reject("contract-violation", violation ?? "does not satisfy the contract");
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
