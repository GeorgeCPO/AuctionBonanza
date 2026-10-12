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
  // Set only on a closed auction that sold. A closed auction without one
  // closed unsold.
  winningBidId: string | null;
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
  winningBidId: string | null;
  createdAt: string;
  updatedAt: string;
  phase: AuctionPhase;
}

// What a live view of an auction displays, at one version. The version and the
// leading bid come from the same auctions row, so a reader can never pair one
// version with the leading bid of another.
export interface AuctionLiveState {
  auctionId: string;
  auctionVersion: string;
  currencyCode: string;
  startsAt: Date;
  endsAt: Date;
  cancelledAt: Date | null;
  closedAt: Date | null;
  phase: AuctionPhase;
  leadingBid: { bidId: string; amountMinor: string } | null;
}

export interface AuctionLiveStateJson {
  auctionId: string;
  auctionVersion: string;
  currencyCode: string;
  startsAt: string;
  endsAt: string;
  cancelledAt: string | null;
  closedAt: string | null;
  phase: AuctionPhase;
  leadingBid: { bidId: string; amountMinor: string } | null;
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
  if (facts.cancelledAt !== null) {
    return "cancelled";
  }
  if (facts.closedAt !== null) {
    return "closed";
  }
  if (observedAt < facts.startsAt) {
    return "scheduled";
  }
  if (observedAt < facts.endsAt) {
    return "open";
  }
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

export function toAuctionLiveStateJson(state: AuctionLiveState): AuctionLiveStateJson {
  return {
    ...state,
    startsAt: state.startsAt.toISOString(),
    endsAt: state.endsAt.toISOString(),
    cancelledAt: state.cancelledAt?.toISOString() ?? null,
    closedAt: state.closedAt?.toISOString() ?? null,
  };
}
