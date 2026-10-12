import type { BidAcceptedV1 } from "@auction-bonanza/contracts";

import type { AuctionLiveState } from "./live-states.js";

// Why the edge started over for an auction after it was live or syncing.
export type ResyncReason = "gap" | "bus-interrupted" | "subscription-lost" | "buffer-overflow";

// "syncing": the state is not trusted yet (waiting for a subscription or a
// read), so events are buffered. "live": events are applied in version order
// and sent to clients.
export type SyncStatus = "syncing" | "live";

// What the edge last showed for an auction.
export type KnownAuctionState = { kind: "found"; state: AuctionLiveState } | { kind: "missing" };

export type EventCounter = "eventsBuffered" | "eventsApplied" | "eventsDropped";

// What the registry must do after AuctionSync handled an input. AuctionSync
// only describes these; the registry carries them out, in order, after the
// state is final.
export type SyncEffect =
  | { kind: "count"; counter: EventCounter }
  // Send every watcher this accepted bid as a change.
  | { kind: "send-bid"; bid: BidAcceptedV1 }
  // The auction is live again: reset the retry backoff and send every watcher
  // a snapshot of the new state.
  | { kind: "synced" }
  // Begin a new attempt to read the state. `afterBackoff` asks for a delay,
  // because the previous attempt did not settle. `receivedVersion` is set
  // when the cause was an event that did not fit, for the log.
  | {
      kind: "start-sync";
      reason: ResyncReason | "initial";
      afterBackoff: boolean;
      receivedVersion?: bigint;
    };
