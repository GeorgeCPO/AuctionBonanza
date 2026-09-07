export type AuctionPhase = "scheduled" | "open" | "awaiting_finalization" | "cancelled" | "closed";

export interface Auction {
  id: string;
  ownerId: string;
  title: string;
  description: string;
  currencyCode: string;
  startsAt: Date;
  endsAt: Date;
  startingPriceMinor: string;
  minimumBidIncrementMinor: string;
  version: string;
  cancelledAt: Date | null;
  closedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
  phase: AuctionPhase;
}

export interface AuctionJson {
  id: string;
  ownerId: string;
  title: string;
  description: string;
  currencyCode: string;
  startsAt: string;
  endsAt: string;
  startingPriceMinor: string;
  minimumBidIncrementMinor: string;
  version: string;
  cancelledAt: string | null;
  closedAt: string | null;
  createdAt: string;
  updatedAt: string;
  phase: AuctionPhase;
}

export function toAuctionJson(auction: Auction): AuctionJson {
  return {
    ...auction,
    startsAt: auction.startsAt.toISOString(),
    endsAt: auction.endsAt.toISOString(),
    cancelledAt: auction.cancelledAt?.toISOString() ?? null,
    closedAt: auction.closedAt?.toISOString() ?? null,
    createdAt: auction.createdAt.toISOString(),
    updatedAt: auction.updatedAt.toISOString(),
  };
}
