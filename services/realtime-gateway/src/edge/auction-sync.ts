import type { BusAuctionEvent } from "../bus/bus.types.js";
import type { LiveStateLookup } from "./live-states.types.js";
import type {
  KnownAuctionState,
  ResyncReason,
  SyncEffect,
  SyncStatus,
} from "./auction-sync.types.js";

// The rules that keep one auction's state correct. It owns that state (what
// is known, at which version, and the events buffered while syncing) and
// changes it only through three inputs: an event arrived (onBusEvent), a
// live-state read finished (onLiveStateRead), or something made the state
// untrustworthy (requestResync). Each returns the effects the caller must
// carry out, as data, after the state is final.
//
// Nothing here waits, sets a timer, or touches the network, so a call runs
// to completion before anything else can happen, and the class can be read
// and tested without mocks. See "Edge" in docs/REALTIME_GATEWAY.md.
//
// With current version c and incoming version v, once live: v <= c is
// dropped, v = c + 1 is applied and sent, and v > c + 1 is a gap that
// resyncs the auction. While syncing, events are buffered; the read that
// ends the sync is the base, and the buffered events are folded into it.
export class AuctionSync {
  private currentStatus: SyncStatus = "syncing";
  private lastKnown: KnownAuctionState | undefined;
  // The version of `lastKnown`. An auction that does not exist is at 0, so
  // any event for it is a gap.
  private currentVersion = 0n;
  // Events received while syncing, in arrival order.
  private buffer: BusAuctionEvent[] = [];

  constructor(private readonly maxBufferedEvents: number) {}

  get status(): SyncStatus {
    return this.currentStatus;
  }

  // The last state shown to clients; undefined before the first read.
  get known(): KnownAuctionState | undefined {
    return this.lastKnown;
  }

  get version(): bigint {
    return this.currentVersion;
  }

  get bufferedEvents(): readonly BusAuctionEvent[] {
    return this.buffer;
  }

  // Asks for the first read. Call once, when the auction starts being watched.
  begin(): SyncEffect[] {
    return [{ kind: "start-sync", reason: "initial", afterBackoff: false }];
  }

  // A validated event arrived on the auction's channel.
  onBusEvent(event: BusAuctionEvent): SyncEffect[] {
    if (this.currentStatus === "syncing") {
      this.buffer.push(event);
      const effects: SyncEffect[] = [{ kind: "count", counter: "eventsBuffered" }];
      // A buffer with holes looks complete and is not, so an overflowing one
      // is thrown away entirely and a new read replaces it.
      if (this.buffer.length > this.maxBufferedEvents) {
        this.buffer = [];
        return this.resync("buffer-overflow", effects);
      }
      return effects;
    }

    const effects: SyncEffect[] = [];
    const outcome = this.applyEvent(event, effects);
    if (outcome === "gap") {
      // Keep the event: a read that lags behind it can still be completed
      // with it, instead of going live a version behind.
      this.buffer.push(event);
      return this.resync("gap", effects, event.version);
    }
    return effects;
  }

  // A live-state read finished. The caller has already checked that it
  // belongs to the current attempt.
  onLiveStateRead(lookup: LiveStateLookup): SyncEffect[] {
    // The read replaces everything known.
    if (lookup.kind === "found") {
      this.lastKnown = { kind: "found", state: lookup.state };
      this.currentVersion = BigInt(lookup.state.auctionVersion);
    } else {
      this.lastKnown = { kind: "missing" };
      this.currentVersion = 0n;
    }

    // The status stays "syncing" while the buffer is folded in, so nothing is
    // sent per event: the snapshot that ends the sync replaces whatever the
    // watchers had anyway.
    this.currentStatus = "syncing";
    const buffered = this.buffer;
    this.buffer = [];
    const effects: SyncEffect[] = [];
    for (const [index, event] of buffered.entries()) {
      const outcome = this.applyEvent(event, effects);
      if (outcome === "gap") {
        // The read is behind the events: keep this event and the rest, read
        // again.
        this.buffer = buffered.slice(index);
        return this.resync("gap", effects);
      }
    }
    this.currentStatus = "live";
    effects.push({ kind: "synced" });
    return effects;
  }

  // The state can no longer be trusted: a connection was lost, or the bus
  // dropped this auction's subscription.
  requestResync(reason: ResyncReason): SyncEffect[] {
    return this.resync(reason, []);
  }

  private resync(
    reason: ResyncReason,
    effects: SyncEffect[],
    receivedVersion?: bigint,
  ): SyncEffect[] {
    // A gap the read did not close, or a buffer that overflowed before the
    // read finished, means the previous sync did not settle. Waiting like a
    // failed read keeps an auction that reads cannot settle (for example,
    // reads that lag the bus) from being read in a tight loop. A gap found
    // while live is repaired at once.
    const afterBackoff =
      this.currentStatus === "syncing" && (reason === "gap" || reason === "buffer-overflow");
    this.currentStatus = "syncing";
    effects.push(
      receivedVersion === undefined
        ? { kind: "start-sync", reason, afterBackoff }
        : { kind: "start-sync", reason, afterBackoff, receivedVersion },
    );
    return effects;
  }

  // Applies one event to the state and adds its effects. A gap changes
  // nothing.
  private applyEvent(event: BusAuctionEvent, effects: SyncEffect[]): "applied" | "dropped" | "gap" {
    if (this.lastKnown?.kind === "found" && event.version <= this.currentVersion) {
      effects.push({ kind: "count", counter: "eventsDropped" });
      return "dropped";
    }
    // A missing auction has no state to apply to, and an event type this edge
    // does not know changed the auction in a way it cannot reproduce.
    if (
      this.lastKnown?.kind !== "found" ||
      event.version !== this.currentVersion + 1n ||
      event.kind !== "bid-accepted"
    ) {
      return "gap";
    }

    const { event: bid } = event;
    this.lastKnown = {
      kind: "found",
      state: {
        ...this.lastKnown.state,
        auctionVersion: bid.auctionVersion,
        leadingBid: { bidId: bid.bidId, amountMinor: bid.amountMinor },
      },
    };
    this.currentVersion = event.version;
    effects.push({ kind: "count", counter: "eventsApplied" });
    if (this.currentStatus === "live") {
      effects.push({ kind: "send-bid", bid });
    }
    return "applied";
  }
}
