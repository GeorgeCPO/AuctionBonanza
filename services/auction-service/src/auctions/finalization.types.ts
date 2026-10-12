import type { Logger } from "pino";

import type { RetryBackoffPolicy } from "../shared/backoff.js";

// What one finalization call did, reported as data for the caller to log or
// count. `winningBidId` is null when the auction closed unsold. `close_failed`
// means the auction could not be closed: the failure is already recorded and
// the auction will not be claimed again before `retryDelayMs` has passed.
export type FinalizationOutcome =
  | { kind: "none_due" }
  | { kind: "closed"; auctionId: string; auctionVersion: string; winningBidId: string | null }
  | {
      kind: "close_failed";
      auctionId: string;
      attempt: number;
      retryDelayMs: number;
      error: Error;
    };

export interface FinalizationRepositoryOptions {
  // Schedules the next attempt at an auction whose close failed.
  retryBackoff: RetryBackoffPolicy;
  random?: () => number;
}

export interface FinalizationRepository {
  // Closes at most one due auction in its own transaction. An auction whose
  // close fails is recorded as failed and reported as `close_failed`; only a
  // failure to reach or write the database throws.
  finalizeNextDue(): Promise<FinalizationOutcome>;
}

export interface FinalizationWorkerOptions {
  repository: FinalizationRepository;
  pollIntervalMs: number;
  retryBackoff: RetryBackoffPolicy;
  logger: Logger;
  random?: () => number;
}
