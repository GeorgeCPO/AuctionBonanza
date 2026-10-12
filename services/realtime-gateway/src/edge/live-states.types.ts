import type { AuctionLiveState } from "./live-states.js";

export type LiveStateLookup = { kind: "found"; state: AuctionLiveState } | { kind: "missing" };

export interface LiveStateSource {
  load(auctionId: string): Promise<LiveStateLookup>;
}

// One bulk read of live states, keyed by auction id.
export interface LiveStateFetcher {
  fetch(auctionIds: readonly string[]): Promise<Map<string, LiveStateLookup>>;
}

export interface LiveStateClientOptions {
  auctionServiceUrl: string;
  timeoutMs: number;
}

export interface LiveStateBatcherOptions {
  client: LiveStateFetcher;
  // How long the first request of a batch waits for others to join it.
  batchDelayMs: number;
  // Requests to auction-service in flight at once, so that a mass resync
  // (every auction after a Valkey reconnect) arrives as a bounded stream.
  maxConcurrentRequests: number;
}

export interface PendingLoad {
  promise: Promise<LiveStateLookup>;
  resolve: (lookup: LiveStateLookup) => void;
  reject: (error: unknown) => void;
}
