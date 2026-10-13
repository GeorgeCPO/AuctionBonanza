import type { AuctionClosedV1 } from "@auction-bonanza/contracts";

// A consumed record reduced to what the decision reads.
export interface DecisionInput {
  key: string | null;
  value: string | null;
  // The `event-type` and `event-id` headers; undefined when absent.
  eventType: string | undefined;
  eventId: string | undefined;
}

// Why a record can never be settled. Each reason is also written to the
// dead-letter record, so a person can group and count them.
export type RejectionReason =
  | "missing-event-type"
  | "missing-value"
  | "invalid-json"
  | "contract-violation"
  | "key-mismatch"
  | "event-id-mismatch";

// An event type settlement does not own. `expected` is true for the type known
// to be safe to ignore (every BidAccepted.v1); any other type is ignored too,
// but is worth a warning, because a close under a new name would be lost.
export interface IgnoreDecision {
  kind: "ignore";
  eventType: string;
  expected: boolean;
}

export interface SettleDecision {
  kind: "settle";
  event: AuctionClosedV1;
}

export interface RejectDecision {
  kind: "reject";
  reason: RejectionReason;
  detail: string;
}

export type SettlementDecision = IgnoreDecision | SettleDecision | RejectDecision;
