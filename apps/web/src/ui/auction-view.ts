import type { AuctionJson } from "../api/api.types.js";
import type { LiveEntry } from "../live/live-auction-book.types.js";
import { derivePhase } from "../live/phase.js";
import type { AuctionView } from "./auction-view.types.js";

// What to show for one auction: the live state when the stream has one, else
// the row fetched over HTTP (which has no leading bid, only the starting price).
export function viewAuction(
  auction: AuctionJson,
  entry: LiveEntry | undefined,
  serverNowMs: number,
): AuctionView {
  if (entry?.kind === "live") {
    const live = entry.auction;
    return {
      phase: derivePhase(live, serverNowMs),
      priceMinor: live.leadingBid?.amountMinor ?? auction.startingPriceMinor,
      hasBid: live.leadingBid !== null,
      currencyCode: live.currencyCode,
      endsAt: live.endsAt,
      version: live.version,
    };
  }
  return {
    phase: derivePhase(auction, serverNowMs),
    priceMinor: auction.startingPriceMinor,
    hasBid: false,
    currencyCode: auction.currencyCode,
    endsAt: auction.endsAt,
    version: undefined,
  };
}
