import type {
  ApplyOutcome,
  LiveAuction,
  LiveEntry,
  LiveEvent,
  SnapshotData,
} from "./live-auction-book.types.js";

// The client's view of every auction it watches, built from the gateway's
// events. The rules are the ones in docs/REALTIME_GATEWAY.md:
//   - a snapshot replaces the auction's state;
//   - a bid-accepted event applies only if its version is greater than the one
//     held, so a duplicate or reordered event cannot move the price backwards.
// Versions are decimal strings that can exceed 2^53, so they are compared as
// BigInt.
export class LiveAuctionBook {
  private readonly entries = new Map<string, LiveEntry>();

  apply(event: LiveEvent): ApplyOutcome {
    switch (event.kind) {
      case "snapshot":
        this.entries.set(event.data.auctionId, {
          kind: "live",
          auction: this.fromSnapshot(event.data),
        });
        return "replaced";
      case "unavailable":
        this.entries.set(event.data.auctionId, { kind: "unavailable" });
        return "replaced";
      case "bid-accepted": {
        const bid = event.data;
        const held = this.entries.get(bid.auctionId);
        if (held?.kind !== "live") {
          return "ignored";
        }
        const isNewer = BigInt(bid.auctionVersion) > BigInt(held.auction.version);
        if (!isNewer) {
          return "stale";
        }
        this.entries.set(bid.auctionId, {
          kind: "live",
          auction: {
            ...held.auction,
            version: bid.auctionVersion,
            currencyCode: bid.currencyCode,
            leadingBid: { bidId: bid.bidId, amountMinor: bid.amountMinor },
          },
        });
        return "applied";
      }
    }
  }

  // Forgets every auction not in `ids`, for when the watched set changes.
  retain(ids: ReadonlySet<string>): void {
    for (const id of this.entries.keys()) {
      if (!ids.has(id)) {
        this.entries.delete(id);
      }
    }
  }

  // A copy, so a caller can keep it as an immutable view while the book moves on.
  entriesSnapshot(): ReadonlyMap<string, LiveEntry> {
    return new Map(this.entries);
  }

  private fromSnapshot(data: SnapshotData): LiveAuction {
    return {
      auctionId: data.auctionId,
      version: data.auctionVersion,
      phase: data.phase,
      currencyCode: data.currencyCode,
      startsAt: data.startsAt,
      endsAt: data.endsAt,
      cancelledAt: data.cancelledAt,
      closedAt: data.closedAt,
      leadingBid: data.leadingBid,
    };
  }
}
