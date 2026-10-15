// Response and request shapes of the backend HTTP APIs. They are written out
// by hand because each service keeps its own schemas private. Ids, versions
// and money amounts are decimal strings, exactly as the services send them.

export type AuctionPhase = "scheduled" | "open" | "awaiting_finalization" | "cancelled" | "closed";

export interface UserJson {
  id: string;
  email: string;
  username: string | null;
}

export interface SessionCredentials {
  accessToken: string;
  refreshToken: string;
  refreshExpiresAt: string;
  tokenType: "Bearer";
  // Seconds until the access token expires.
  expiresIn: number;
  user: UserJson;
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

export interface AuctionListJson {
  items: AuctionJson[];
  nextCursor: string | null;
}

export interface LeadingBidJson {
  bidId: string;
  amountMinor: string;
}

export interface LiveStateJson {
  auctionId: string;
  auctionVersion: string;
  currencyCode: string;
  startsAt: string;
  endsAt: string;
  cancelledAt: string | null;
  closedAt: string | null;
  phase: AuctionPhase;
  leadingBid: LeadingBidJson | null;
}

export interface LiveStatesJson {
  items: LiveStateJson[];
  missingIds: string[];
  observedAt: string;
}

export interface CreateAuctionRequest {
  title: string;
  description: string;
  currencyCode: string;
  endsAt: string;
  startingPriceMinor: string;
  minimumBidIncrementMinor: string;
}

export interface BidJson {
  id: string;
  auctionId: string;
  bidderId: string;
  amountMinor: string;
  idempotencyKey: string;
  createdAt: string;
}

export interface PlaceBidResponse {
  bid: BidJson;
  auction: { id: string; version: string; leadingBidId: string | null; updatedAt: string };
}

// Error bodies are `{ code, message }`, with extra fields for some codes.
export interface ApiErrorBody {
  code: string;
  message: string;
  [extra: string]: unknown;
}
