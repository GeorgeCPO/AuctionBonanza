import { auctionChannel } from "../bus/bus.js";
import type { AuctionWatcher } from "./auction-registry.types.js";
import { AuctionSync } from "./auction-sync.js";

// One auction the edge is serving: its clients, its state, and everything
// that takes time around them (the subscription, the retry and linger
// timers, and which sync attempt is current). AuctionRegistry creates one per
// watched auction and does the work that needs the outside world.
//
// It never talks to the bus or to auction-service itself; the registry hands
// it the operations to run and the callbacks to schedule.
export class WatchedAuction {
  readonly channel: string;
  // Decides what each message means; see auction-sync.ts.
  readonly sync: AuctionSync;

  private readonly watchers = new Set<AuctionWatcher>();
  // Incremented by every sync attempt. A read finishing for an older attempt
  // is discarded, because it may predate what made the newer one necessary.
  private generation = 0;
  private subscription: Promise<void> | undefined;
  // Consecutive attempts that did not end live, for the retry backoff.
  private backoffAttempts = 0;
  private retryTimer: NodeJS.Timeout | undefined;
  private lingerTimer: NodeJS.Timeout | undefined;
  private discarded = false;

  constructor(
    readonly auctionId: string,
    maxBufferedEvents: number,
  ) {
    this.channel = auctionChannel(auctionId);
    this.sync = new AuctionSync(maxBufferedEvents);
  }

  // True once the auction was removed or the registry closed. Late messages
  // and late reads for it are ignored.
  get isDiscarded(): boolean {
    return this.discarded;
  }

  // Adds a client. A client arriving during the linger period cancels the
  // removal that was counting down.
  addWatcher(watcher: AuctionWatcher): void {
    this.cancelLinger();
    this.watchers.add(watcher);
  }

  // Returns true when this call removed the last client.
  removeWatcher(watcher: AuctionWatcher): boolean {
    const wasWatching = this.watchers.delete(watcher);
    return wasWatching && this.watchers.size === 0;
  }

  // Sends one serialized frame to every client, which is serialized once per
  // auction and change, not once per client.
  sendToWatchers(frame: string): void {
    // A client whose connection overflows unwatches during send; deleting
    // from a Set while iterating it is well defined.
    for (const watcher of this.watchers) {
      watcher.send(frame);
    }
  }

  // Keeps the auction for a while after its last client left: clients
  // switching pages leave and come back quickly, and keeping the subscription
  // avoids churn on Valkey and auction-service.
  startLinger(durationMs: number, onExpire: () => void): void {
    this.cancelLinger();
    this.lingerTimer = setTimeout(onExpire, durationMs);
  }

  // Starts a new sync attempt and returns its number. Any retry scheduled by
  // an earlier attempt is cancelled.
  beginAttempt(): number {
    this.generation += 1;
    clearTimeout(this.retryTimer);
    this.retryTimer = undefined;
    return this.generation;
  }

  // True when the attempt was superseded by a newer one, or the auction is
  // gone. Whatever it still produces must be ignored.
  isStale(attempt: number): boolean {
    return this.discarded || this.generation !== attempt;
  }

  // Counts one more attempt that did not end live, and returns how many in a
  // row there have been.
  nextBackoffAttempt(): number {
    this.backoffAttempts += 1;
    return this.backoffAttempts;
  }

  resetBackoff(): void {
    this.backoffAttempts = 0;
  }

  scheduleRetry(delayMs: number, run: () => void): void {
    clearTimeout(this.retryTimer);
    this.retryTimer = setTimeout(run, delayMs);
  }

  // Runs `open` to subscribe, unless a subscription is already open or being
  // opened, and returns when it is confirmed. A subscription that failed is
  // forgotten, so the next call tries again.
  subscribeOnce(open: () => Promise<void>): Promise<void> {
    if (this.subscription === undefined) {
      const subscription = open();
      this.subscription = subscription;
      subscription.catch(() => {
        if (this.subscription === subscription) {
          this.subscription = undefined;
        }
      });
    }
    return this.subscription;
  }

  // The bus dropped the subscription: the next subscribeOnce opens a new one.
  forgetSubscription(): void {
    this.subscription = undefined;
  }

  // Stops the timers and marks the auction as gone.
  discard(): void {
    this.discarded = true;
    clearTimeout(this.retryTimer);
    this.cancelLinger();
  }

  private cancelLinger(): void {
    clearTimeout(this.lingerTimer);
    this.lingerTimer = undefined;
  }
}
