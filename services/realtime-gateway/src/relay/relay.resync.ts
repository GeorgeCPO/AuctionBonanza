import { setTimeout as delay } from "node:timers/promises";

import type { Logger } from "pino";

import { CONTROL_CHANNEL, serializeResync, type BusPublisher } from "../bus/bus.js";
import { backoffDelayMs, type RetryBackoffPolicy } from "../shared/backoff.js";

export interface ResyncBroadcasterOptions {
  publisher: BusPublisher;
  retryBackoff: RetryBackoffPolicy;
  logger: Logger;
  // Ends background retries once the relay stops.
  signal: AbortSignal;
}

// Tells every edge to refresh all of its auctions when the relay may have
// skipped events. See "Offset resets and resync" in docs/REALTIME_GATEWAY.md.
//
// A resync is pending while more have been requested than published.
// Counters, not a flag, so a request made during a publish is not lost.
// Requests that are pending together are covered by one publish, which
// carries the latest reason.
export class ResyncBroadcaster {
  private readonly options: ResyncBroadcasterOptions;
  private requested = 0;
  private published = 0;
  private latestReason = "";
  private inFlight: Promise<void> | undefined;
  private retrying = false;

  constructor(options: ResyncBroadcasterOptions) {
    this.options = options;
  }

  // The relay publishes a pending resync before it forwards its next message
  // anyway. The request is also published in the background, because a
  // partition with no new messages must not delay it.
  request(reason: string): void {
    this.requested += 1;
    this.latestReason = reason;
    this.options.logger.warn({ reason }, "resync requested; every edge will refresh its auctions");
    void this.retryUntilPublished();
  }

  // Resolves when no resync is pending; rejects if publishing one failed.
  // Concurrent callers share one publish.
  async publishPending(): Promise<void> {
    while (this.isPending()) {
      this.inFlight ??= this.publish().finally(() => {
        this.inFlight = undefined;
      });
      await this.inFlight;
    }
  }

  private async retryUntilPublished(): Promise<void> {
    if (this.retrying) return;
    this.retrying = true;
    const { signal } = this.options;
    let attempt = 0;
    try {
      while (!signal.aborted && this.isPending()) {
        try {
          await this.publishPending();
        } catch (error) {
          attempt += 1;
          const retryDelayMs = backoffDelayMs(this.options.retryBackoff, attempt);
          this.options.logger.error({ err: error, attempt, retryDelayMs }, "resync publish failed");
          await sleep(retryDelayMs, signal);
        }
      }
    } finally {
      this.retrying = false;
    }
  }

  private async publish(): Promise<void> {
    const requested = this.requested;
    const reason = this.latestReason;
    await this.options.publisher.publish(CONTROL_CHANNEL, serializeResync(reason));
    this.published = Math.max(this.published, requested);
    this.options.logger.info({ reason }, "resync published");
  }

  private isPending(): boolean {
    return this.published < this.requested;
  }
}

async function sleep(ms: number, signal: AbortSignal): Promise<void> {
  try {
    await delay(ms, undefined, { signal });
  } catch (error) {
    if (!signal.aborted) throw error;
  }
}
