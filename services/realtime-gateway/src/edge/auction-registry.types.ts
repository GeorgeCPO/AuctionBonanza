import type { Logger } from "pino";

import type { BusSubscriber } from "../bus/bus.types.js";
import type { RetryBackoffPolicy } from "../shared/backoff.js";
import type { ResyncReason } from "./auction-sync.types.js";
import type { LiveStateSource } from "./live-states.types.js";

// One client connection, as the registry sees it. `send` receives complete
// SSE frames; it must not throw.
export interface AuctionWatcher {
  send(frame: string): void;
}

export interface AuctionRegistryOptions {
  bus: BusSubscriber;
  liveStates: LiveStateSource;
  // How long an auction stays subscribed after its last watcher left.
  lingerMs: number;
  // Events buffered per auction while it syncs. Beyond this the buffer is
  // discarded and the sync starts over, since a newer read replaces it.
  maxBufferedEvents: number;
  retryBackoff: RetryBackoffPolicy;
  logger: Logger;
}

export interface AuctionRegistryStats {
  auctions: number;
  // Waiting for a subscription or a live-state read.
  syncing: number;
  // Received while the auction was syncing.
  eventsBuffered: number;
  eventsApplied: number;
  eventsDropped: number;
  resyncs: Record<ResyncReason, number>;
  malformedMessages: number;
}
