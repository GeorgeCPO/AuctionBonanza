import { backoffDelayMs } from "../shared/backoff.js";
import { abortableSleep } from "../shared/sleep.js";
import type { FinalizationWorkerOptions } from "./finalization.types.js";

// Log messages are part of the worker's operational contract: tests match on
// them, and so may log-based alerts.
const LOG_MESSAGES = {
  closed: "auction closed",
  cycleFailed: "auction finalization failed",
} as const;

export class FinalizationWorker {
  constructor(private readonly options: FinalizationWorkerOptions) {}

  // Closes due auctions one transaction at a time until none is left, or the
  // signal aborts between two closes, and returns how many it closed. A
  // failure throws and leaves the failed auction unclosed, so the next call
  // claims it again.
  async drain(signal?: AbortSignal): Promise<number> {
    let closed = 0;

    while (signal?.aborted !== true) {
      const outcome = await this.options.repository.finalizeNextDue();
      if (outcome.kind === "none_due") {
        return closed;
      }
      closed += 1;
      this.options.logger.info(
        {
          auctionId: outcome.auctionId,
          auctionVersion: outcome.auctionVersion,
          winningBidId: outcome.winningBidId,
        },
        LOG_MESSAGES.closed,
      );
    }
    return closed;
  }

  // Runs until the signal aborts. An in-flight close always finishes: it is a
  // few statements long, and abandoning it would only roll it back.
  async run(signal: AbortSignal): Promise<void> {
    let consecutiveFailures = 0;

    while (!signal.aborted) {
      let pauseMs: number;
      try {
        await this.drain(signal);
        consecutiveFailures = 0;
        pauseMs = this.options.pollIntervalMs;
      } catch (error) {
        consecutiveFailures += 1;
        pauseMs = backoffDelayMs(
          this.options.retryBackoff,
          consecutiveFailures,
          this.options.random,
        );
        this.options.logger.error(
          { err: error, consecutiveFailures, retryDelayMs: pauseMs },
          LOG_MESSAGES.cycleFailed,
        );
      }

      await abortableSleep(pauseMs, signal);
    }
  }
}
