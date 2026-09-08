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

export interface AuctionPhaseFacts {
  cancelledAt: Date | null;
  closedAt: Date | null;
  startsAt: Date;
  endsAt: Date;
}

// Shared by every read of auction state (list/findById/bid acceptance) so
// the phase precedence documented in docs/AUCTION_LIFECYCLE.md is defined
// exactly once. `observedAt` must be captured under a row lock via
// `clock_timestamp()` for any time-sensitive transition, never `now()`.
export function derivePhase(facts: AuctionPhaseFacts, observedAt: Date): AuctionPhase {
  if (facts.cancelledAt !== null) return "cancelled";
  if (facts.closedAt !== null) return "closed";
  if (observedAt < facts.startsAt) return "scheduled";
  if (observedAt < facts.endsAt) return "open";
  return "awaiting_finalization";
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
