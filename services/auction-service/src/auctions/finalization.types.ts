import type { Logger } from "pino";

import type { RetryBackoffPolicy } from "../shared/backoff.js";

// What one finalization call did, reported as data for the caller to log or
// count. `winningBidId` is null when the auction closed unsold.
export type FinalizationOutcome =
  | { kind: "none_due" }
  | { kind: "closed"; auctionId: string; auctionVersion: string; winningBidId: string | null };

export interface FinalizationRepository {
  // Closes at most one due auction in its own transaction.
  finalizeNextDue(): Promise<FinalizationOutcome>;
}

export interface FinalizationWorkerOptions {
  repository: FinalizationRepository;
  pollIntervalMs: number;
  retryBackoff: RetryBackoffPolicy;
  logger: Logger;
  random?: () => number;
}
