import type { DisplayPhase } from "../live/phase.types.js";

export interface AuctionView {
  phase: DisplayPhase;
  // The leading bid, or the starting price while nobody has bid.
  priceMinor: string;
  hasBid: boolean;
  currencyCode: string;
  // Time left while the auction is open; undefined otherwise.
  countdown: string | undefined;
  version: string | undefined;
}
