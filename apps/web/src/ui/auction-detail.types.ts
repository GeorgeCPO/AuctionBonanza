export interface PendingBid {
  amountMinor: string;
  idempotencyKey: string;
}

export interface BidResult {
  ok: boolean;
  text: string;
}
