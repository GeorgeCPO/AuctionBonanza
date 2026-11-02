import type { AuctionJson } from "../api/api.types.js";
import type { LiveEntry } from "../live/live-auction-book.types.js";
import { derivePhase, formatCountdown } from "../live/phase.js";
import type { AuctionView } from "./auction-view.types.js";

// What to show for one auction: the live state when the stream has one, else
// the row fetched over HTTP (which has no leading bid, only the starting price).
export function viewAuction(
  auction: AuctionJson,
  entry: LiveEntry | undefined,
  serverNowMs: number,
): AuctionView {
  const live = entry?.kind === "live" ? entry.auction : undefined;
  const source = live ?? auction;
  // closedAt and cancelledAt only ever go from null to set, and the stream
  // sends no event for either, so a snapshot can predate them. A value on the
  // fetched row counts too.
  const phase = derivePhase(
    {
      startsAt: source.startsAt,
      endsAt: source.endsAt,
      cancelledAt: source.cancelledAt ?? auction.cancelledAt,
      closedAt: source.closedAt ?? auction.closedAt,
    },
    serverNowMs,
  );
  const leadingBid = live?.leadingBid ?? null;
  return {
    phase,
    priceMinor: leadingBid?.amountMinor ?? auction.startingPriceMinor,
    hasBid: leadingBid !== null,
    currencyCode: source.currencyCode,
    countdown: phase === "open" ? formatCountdown(source.endsAt, serverNowMs) : undefined,
    version: live?.version,
  };
}
