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

// An event type settlement does not own, such as every BidAccepted.v1.
export interface IgnoreDecision {
  kind: "ignore";
  eventType: string;
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
