import type { AuctionClosedV1 } from "@auction-bonanza/contracts";

// What settling one event did. Every outcome is a normal result, so the
// caller can log and count each without treating any of them as a failure.
export type SettlementOutcome =
  // The auction was sold: its order now exists.
  | { kind: "order_created"; orderId: string }
  // The auction closed with no bid: there is nothing to settle, and the event
  // is recorded as handled.
  | { kind: "unsold_recorded" }
  // This event was handled before, by an earlier delivery. Nothing changed.
  | { kind: "duplicate" };

export interface SettlementRepository {
  // Settles one AuctionClosed.v1 in a single PostgreSQL transaction, so the
  // event is recorded as handled if and only if its effect exists.
  //
  // Throws PermanentSettlementError when the database refuses the event's
  // data. Any other error is transient: nothing was written, and settling the
  // same event again is safe.
  settle(event: AuctionClosedV1): Promise<SettlementOutcome>;
}
