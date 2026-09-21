export interface Bid {
  id: string;
  auctionId: string;
  bidderId: string;
  amountMinor: string;
  idempotencyKey: string;
  createdAt: Date;
}

export interface BidJson {
  id: string;
  auctionId: string;
  bidderId: string;
  amountMinor: string;
  idempotencyKey: string;
  createdAt: string;
}

export function toBidJson(bid: Bid): BidJson {
  return {
    ...bid,
    createdAt: bid.createdAt.toISOString(),
  };
}
