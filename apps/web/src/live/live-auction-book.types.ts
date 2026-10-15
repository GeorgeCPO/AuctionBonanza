import type { AuctionPhase, LeadingBidJson } from "../api/api.types.js";

// The `data` of the gateway's SSE frames (docs/REALTIME_GATEWAY.md).
export interface SnapshotData {
  auctionId: string;
  auctionVersion: string;
  phase: AuctionPhase;
  currencyCode: string;
  startsAt: string;
  endsAt: string;
  cancelledAt: string | null;
  closedAt: string | null;
  leadingBid: LeadingBidJson | null;
  serverTime: string;
}

export interface BidAcceptedData {
  auctionId: string;
  auctionVersion: string;
  bidId: string;
  amountMinor: string;
  currencyCode: string;
  occurredAt: string;
}

export interface UnavailableData {
  auctionId: string;
  reason: string;
}

export type LiveEvent =
  | { kind: "snapshot"; data: SnapshotData }
  | { kind: "bid-accepted"; data: BidAcceptedData }
  | { kind: "unavailable"; data: UnavailableData };

export interface LiveAuction {
  auctionId: string;
  version: string;
  phase: AuctionPhase;
  currencyCode: string;
  startsAt: string;
  endsAt: string;
  cancelledAt: string | null;
  closedAt: string | null;
  leadingBid: LeadingBidJson | null;
}

export type LiveEntry = { kind: "live"; auction: LiveAuction } | { kind: "unavailable" };

// What applying an event did. `stale` and `ignored` change nothing.
export type ApplyOutcome = "applied" | "replaced" | "stale" | "ignored";
