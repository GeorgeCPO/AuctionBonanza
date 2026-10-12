import { describe, expect, it, vi } from "vitest";

import type { Bid } from "../../src/bids/bid.js";
import type { BidRepository, PlacedAuctionState } from "../../src/bids/bid.repository.js";
import { BidService } from "../../src/bids/bid.service.js";

const bid: Bid = {
  id: "500",
  auctionId: "91",
  bidderId: "43",
  amountMinor: "1100",
  idempotencyKey: "key-1",
  createdAt: new Date("2030-01-01T10:05:00.000Z"),
};
const auctionState: PlacedAuctionState = {
  id: "91",
  version: "2",
  leadingBidId: "500",
  updatedAt: new Date("2030-01-01T10:05:00.000Z"),
};

function makeRepository(placeBid: BidRepository["placeBid"]): BidRepository {
  return { placeBid };
}

describe("BidService", () => {
  it("rejects a malformed amount before calling the repository", async () => {
    const placeBid = vi.fn<BidRepository["placeBid"]>();
    const service = new BidService(makeRepository(placeBid));

    const outcome = await service.create("91", "43", {
      amountMinor: "0",
      idempotencyKey: "key-1",
    });

    expect(outcome).toMatchObject({ kind: "invalid" });
    expect(placeBid).not.toHaveBeenCalled();
  });

  it.each(["", " ", "k".repeat(101)])(
    "rejects an idempotency key of length %j before calling the repository",
    async (idempotencyKey) => {
      const placeBid = vi.fn<BidRepository["placeBid"]>();
      const service = new BidService(makeRepository(placeBid));

      const outcome = await service.create("91", "43", { amountMinor: "1100", idempotencyKey });

      expect(outcome).toMatchObject({ kind: "invalid" });
      expect(placeBid).not.toHaveBeenCalled();
    },
  );

  it("passes normalized input through to the repository", async () => {
    const placeBid = vi.fn<BidRepository["placeBid"]>(async () => ({
      kind: "placed",
      bid,
      auction: auctionState,
    }));
    const service = new BidService(makeRepository(placeBid));

    await service.create("91", "43", { amountMinor: "1100", idempotencyKey: "  key-1  " });

    expect(placeBid).toHaveBeenCalledWith({
      auctionId: "91",
      bidderId: "43",
      amountMinor: "1100",
      idempotencyKey: "key-1",
    });
  });

  it("reports a fresh placement with replay: false", async () => {
    const placeBid = vi.fn<BidRepository["placeBid"]>(async () => ({
      kind: "placed",
      bid,
      auction: auctionState,
    }));
    const service = new BidService(makeRepository(placeBid));

    const outcome = await service.create("91", "43", {
      amountMinor: "1100",
      idempotencyKey: "key-1",
    });

    expect(outcome).toEqual({ kind: "placed", bid, auction: auctionState, replay: false });
  });

  it("collapses an idempotency replay into the same placed shape with replay: true", async () => {
    const placeBid = vi.fn<BidRepository["placeBid"]>(async () => ({
      kind: "idempotency_replay",
      bid,
      auction: auctionState,
    }));
    const service = new BidService(makeRepository(placeBid));

    const outcome = await service.create("91", "43", {
      amountMinor: "1100",
      idempotencyKey: "key-1",
    });

    expect(outcome).toEqual({ kind: "placed", bid, auction: auctionState, replay: true });
  });

  it.each([
    { kind: "auction_not_found" as const },
    { kind: "auction_not_open" as const, phase: "closed" as const },
    { kind: "self_bid_not_allowed" as const },
    { kind: "bid_too_low" as const, minimumQualifyingAmountMinor: "1200" },
    { kind: "idempotency_key_reused" as const, originalAmountMinor: "1100" },
  ])("passes the %j outcome through unchanged", async (outcome) => {
    const placeBid = vi.fn<BidRepository["placeBid"]>(async () => outcome);
    const service = new BidService(makeRepository(placeBid));

    await expect(
      service.create("91", "43", { amountMinor: "1100", idempotencyKey: "key-1" }),
    ).resolves.toEqual(outcome);
  });
});
