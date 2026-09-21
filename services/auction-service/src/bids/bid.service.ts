import type { AuctionPhase } from "../auctions/auction.js";
import { parseMinorUnits } from "../shared/money.js";
import type { Bid } from "./bid.js";
import type { BidRepository } from "./bid.repository.js";

export const MAX_IDEMPOTENCY_KEY_LENGTH = 100;

export interface PlaceBidInput {
  amountMinor: string;
  idempotencyKey: string;
}

export interface PlacedBidAuctionState {
  id: string;
  version: string;
  leadingBidId: string;
  updatedAt: Date;
}

export type PlaceBidOutcome =
  | { kind: "placed"; bid: Bid; auction: PlacedBidAuctionState; replay: boolean }
  | { kind: "invalid"; message: string }
  | { kind: "auction_not_found" }
  | { kind: "auction_not_open"; phase: Exclude<AuctionPhase, "open"> }
  | { kind: "self_bid_not_allowed" }
  | { kind: "bid_too_low"; minimumQualifyingAmountMinor: string }
  | { kind: "idempotency_key_reused"; originalAmountMinor: string };

export interface BidServicePort {
  create(auctionId: string, bidderId: string, input: PlaceBidInput): Promise<PlaceBidOutcome>;
}

export class BidService implements BidServicePort {
  constructor(private readonly repository: BidRepository) {}

  async create(
    auctionId: string,
    bidderId: string,
    input: PlaceBidInput,
  ): Promise<PlaceBidOutcome> {
    const amountMinor = parseMinorUnits(input.amountMinor, false);
    if (amountMinor === undefined) {
      return {
        kind: "invalid",
        message: "amountMinor must be a positive integer within the supported range",
      };
    }

    const idempotencyKey = input.idempotencyKey.trim();
    if (idempotencyKey.length < 1 || idempotencyKey.length > MAX_IDEMPOTENCY_KEY_LENGTH) {
      return {
        kind: "invalid",
        message: `idempotencyKey must be between 1 and ${MAX_IDEMPOTENCY_KEY_LENGTH} characters`,
      };
    }

    const outcome = await this.repository.placeBid({
      auctionId,
      bidderId,
      amountMinor,
      idempotencyKey,
    });

    switch (outcome.kind) {
      case "placed":
        return { kind: "placed", bid: outcome.bid, auction: outcome.auction, replay: false };
      case "idempotency_replay":
        return { kind: "placed", bid: outcome.bid, auction: outcome.auction, replay: true };
      default:
        return outcome;
    }
  }
}
