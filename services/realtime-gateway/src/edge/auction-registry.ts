import type { Logger } from "pino";

import { parseAuctionEventEnvelope } from "../bus/bus.js";
import type { BusAuctionEvent } from "../bus/bus.types.js";
import { backoffDelayMs } from "../shared/backoff.js";
import type {
  AuctionRegistryOptions,
  AuctionRegistryStats,
  AuctionWatcher,
} from "./auction-registry.types.js";
import type { ResyncReason, SyncEffect } from "./auction-sync.types.js";
import { sseEvent } from "./sse.js";
import { WatchedAuction } from "./watched-auction.js";

// The edge's per-auction state, shared by every client of this process.
// See "Edge" in docs/REALTIME_GATEWAY.md.
//
// The first watcher of an auction subscribes to its channel, then reads its
// live state, then applies the events buffered meanwhile. Subscribing first
// means an event committed during the read is either reflected in the read
// or buffered.
//
// Each auction is a WatchedAuction: its clients, its timers, and an
// AuctionSync that owns its state and decides what each message means by
// returning the effects to carry out. This class keeps the map of auctions
// and does the work that needs the outside world: subscribing on the bus,
// reading live states, carrying out the effects. Because a call to
// AuctionSync and the effects that follow it run without waiting, no message
// can arrive between them.
export class AuctionRegistry {
  private readonly entries = new Map<string, WatchedAuction>();
  private readonly logger: Logger;
  private readonly counters: Omit<AuctionRegistryStats, "auctions" | "syncing"> = {
    eventsBuffered: 0,
    eventsApplied: 0,
    eventsDropped: 0,
    resyncs: { gap: 0, "bus-interrupted": 0, "subscription-lost": 0, "buffer-overflow": 0 },
    malformedMessages: 0,
  };
  private closed = false;

  constructor(private readonly options: AuctionRegistryOptions) {
    this.logger = options.logger.child({ component: "auction-registry" });
    options.bus.onInterruption(() => this.resyncAll("bus-interrupted"));
    options.bus.onSubscriptionLost((channel) => this.resubscribe(channel));
  }

  // Adds a watcher. It receives the auction's current snapshot right away if
  // the edge has one (possibly while a resync is under way, in which case a
  // fresh snapshot follows), otherwise once the first read completes.
  watch(auctionId: string, watcher: AuctionWatcher): void {
    if (this.closed) {
      return;
    }
    let entry = this.entries.get(auctionId);
    if (entry === undefined) {
      entry = new WatchedAuction(auctionId, this.options.maxBufferedEvents);
      this.entries.set(auctionId, entry);
      this.runEffects(entry, entry.sync.begin());
    }
    entry.addWatcher(watcher);
    if (entry.sync.known !== undefined) {
      watcher.send(this.snapshotFrame(entry));
    }
  }

  unwatch(auctionId: string, watcher: AuctionWatcher): void {
    const entry = this.entries.get(auctionId);
    if (entry === undefined) {
      return;
    }
    const wasLastWatcher = entry.removeWatcher(watcher);
    if (!wasLastWatcher) {
      return;
    }
    entry.startLinger(this.options.lingerMs, () => this.remove(entry));
  }

  stats(): AuctionRegistryStats {
    let syncing = 0;
    for (const entry of this.entries.values()) {
      if (entry.sync.status === "syncing") {
        syncing += 1;
      }
    }
    return {
      auctions: this.entries.size,
      syncing,
      ...this.counters,
      resyncs: { ...this.counters.resyncs },
    };
  }

  // Stops all timers. The bus connection is closed by its owner, which drops
  // every subscription with it.
  close(): void {
    this.closed = true;
    for (const entry of this.entries.values()) {
      entry.discard();
    }
    this.entries.clear();
  }

  private resyncAll(reason: ResyncReason): void {
    if (this.entries.size === 0) {
      return;
    }
    this.logger.warn({ reason, auctions: this.entries.size }, "resyncing every watched auction");
    for (const entry of this.entries.values()) {
      this.runEffects(entry, entry.sync.requestResync(reason));
    }
  }

  // The bus dropped one channel's listener, so the auction must subscribe
  // again before anything it shows can be trusted.
  private resubscribe(channel: string): void {
    const entry = [...this.entries.values()].find((candidate) => candidate.channel === channel);
    if (entry === undefined) {
      return;
    }
    this.logger.warn({ auctionId: entry.auctionId }, "subscription lost; resubscribing auction");
    entry.forgetSubscription();
    this.runEffects(entry, entry.sync.requestResync("subscription-lost"));
  }

  private runEffects(entry: WatchedAuction, effects: readonly SyncEffect[]): void {
    for (const effect of effects) {
      switch (effect.kind) {
        case "count":
          this.counters[effect.counter] += 1;
          break;
        case "send-bid":
          entry.sendToWatchers(
            sseEvent("bid-accepted", {
              auctionId: effect.bid.auctionId,
              auctionVersion: effect.bid.auctionVersion,
              bidId: effect.bid.bidId,
              amountMinor: effect.bid.amountMinor,
              currencyCode: effect.bid.currencyCode,
              occurredAt: effect.bid.occurredAt,
            }),
          );
          break;
        case "synced":
          entry.resetBackoff();
          entry.sendToWatchers(this.snapshotFrame(entry));
          break;
        case "start-sync":
          if (effect.reason !== "initial") {
            this.counters.resyncs[effect.reason] += 1;
          }
          if (effect.receivedVersion !== undefined) {
            this.logger.info(
              {
                auctionId: entry.auctionId,
                version: entry.sync.version,
                received: effect.receivedVersion,
              },
              "version gap; resyncing auction",
            );
          }
          this.startSync(entry, effect.afterBackoff);
          break;
      }
    }
  }

  private startSync(entry: WatchedAuction, afterBackoff: boolean): void {
    const attempt = entry.beginAttempt();
    if (afterBackoff) {
      this.retryLater(entry, attempt, "auction did not settle; resyncing after backoff");
    } else {
      void this.sync(entry, attempt);
    }
  }

  private async sync(entry: WatchedAuction, attempt: number): Promise<void> {
    try {
      await entry.subscribeOnce(() =>
        this.options.bus.subscribe(entry.channel, (message) => this.receive(entry, message)),
      );
      // After a lost connection, read only once it has resubscribed;
      // otherwise an event published in between is neither in the read nor
      // delivered.
      await this.options.bus.whenRestored();
      if (entry.isStale(attempt)) {
        return;
      }
      const lookup = await this.options.liveStates.load(entry.auctionId);
      if (entry.isStale(attempt)) {
        return;
      }
      this.runEffects(entry, entry.sync.onLiveStateRead(lookup));
    } catch (error) {
      if (entry.isStale(attempt)) {
        return;
      }
      this.retryLater(entry, attempt, "auction sync failed; retrying", error);
    }
  }

  // Consecutive syncs that do not end live back off; the count resets once
  // the auction is live again.
  private retryLater(
    entry: WatchedAuction,
    attempt: number,
    message: string,
    error?: unknown,
  ): void {
    const backoffAttempt = entry.nextBackoffAttempt();
    const retryDelayMs = backoffDelayMs(this.options.retryBackoff, backoffAttempt);
    this.logger.warn(
      { err: error, auctionId: entry.auctionId, attempt: backoffAttempt, retryDelayMs },
      message,
    );
    entry.scheduleRetry(retryDelayMs, () => void this.sync(entry, attempt));
  }

  private receive(entry: WatchedAuction, message: string): void {
    if (entry.isDiscarded) {
      return;
    }
    let event: BusAuctionEvent;
    try {
      event = parseAuctionEventEnvelope(message);
    } catch (error) {
      this.counters.malformedMessages += 1;
      this.logger.error(
        { err: error, auctionId: entry.auctionId },
        "ignoring malformed bus message; the next event will reveal any gap",
      );
      return;
    }
    if (event.auctionId !== entry.auctionId) {
      this.counters.malformedMessages += 1;
      this.logger.error(
        { auctionId: entry.auctionId, eventAuctionId: event.auctionId },
        "ignoring bus message for another auction",
      );
      return;
    }

    this.runEffects(entry, entry.sync.onBusEvent(event));
  }

  private snapshotFrame(entry: WatchedAuction): string {
    const { known } = entry.sync;
    if (known?.kind !== "found") {
      return sseEvent("unavailable", { auctionId: entry.auctionId, reason: "not-found" });
    }
    const { state } = known;
    return sseEvent("snapshot", {
      auctionId: state.auctionId,
      auctionVersion: state.auctionVersion,
      phase: state.phase,
      currencyCode: state.currencyCode,
      startsAt: state.startsAt,
      endsAt: state.endsAt,
      cancelledAt: state.cancelledAt,
      closedAt: state.closedAt,
      leadingBid: state.leadingBid,
      serverTime: new Date().toISOString(),
    });
  }

  private remove(entry: WatchedAuction): void {
    this.entries.delete(entry.auctionId);
    entry.discard();
    // The bus orders this after any pending subscribe of the channel, and a
    // later subscribe of the same channel after this.
    this.options.bus.unsubscribe(entry.channel).catch((error: unknown) => {
      this.logger.warn({ err: error, auctionId: entry.auctionId }, "unsubscribe failed");
    });
  }
}
