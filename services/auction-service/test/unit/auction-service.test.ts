import { describe, expect, it, vi } from "vitest";

import type { AuctionRepository } from "../../src/auctions/auction.repository.js";
import { AuctionService } from "../../src/auctions/auction.service.js";

function makeRepository(create: AuctionRepository["create"]): AuctionRepository {
  return {
    create,
    findById: vi.fn<AuctionRepository["findById"]>(async () => undefined),
    list: vi.fn<AuctionRepository["list"]>(async () => []),
    findLiveStates: vi.fn<AuctionRepository["findLiveStates"]>(),
  };
}

const validInput = {
  title: "Vintage clock",
  description: "A mechanical clock.",
  currencyCode: "USD",
  startsAt: "2030-01-01T10:00:00.000Z",
  endsAt: "2030-01-01T11:00:00.000Z",
  startingPriceMinor: "1000",
  minimumBidIncrementMinor: "100",
};

describe("AuctionService", () => {
  it("normalizes text and canonicalizes timestamps before persistence", async () => {
    const create = vi.fn<AuctionRepository["create"]>(async () => ({ kind: "invalid_schedule" }));
    const repository = makeRepository(create);
    const service = new AuctionService(repository, ["EUR", "USD"]);

    await service.create("42", {
      ...validInput,
      title: "  Vintage clock  ",
      description: "  A mechanical clock.  ",
      startsAt: "2030-01-01T12:00:00+02:00",
    });

    expect(create).toHaveBeenCalledWith(
      expect.objectContaining({
        ownerId: "42",
        title: "Vintage clock",
        description: "A mechanical clock.",
        startsAt: new Date("2030-01-01T10:00:00.000Z"),
        endsAt: new Date("2030-01-01T11:00:00.000Z"),
      }),
    );
  });

  it.each([
    [{ ...validInput, currencyCode: "CAD" }, "currencyCode is not supported"],
    [{ ...validInput, startingPriceMinor: "9223372036854775808" }, "startingPriceMinor"],
    [{ ...validInput, minimumBidIncrementMinor: "0" }, "minimumBidIncrementMinor"],
    [{ ...validInput, title: "   " }, "title"],
  ])("rejects invalid domain input before persistence", async (input, message) => {
    const create = vi.fn<AuctionRepository["create"]>(async () => ({ kind: "invalid_schedule" }));
    const repository = makeRepository(create);
    const service = new AuctionService(repository, ["USD"]);

    await expect(service.create("42", input)).resolves.toMatchObject({
      kind: "invalid",
      message: expect.stringContaining(message),
    });
    expect(create).not.toHaveBeenCalled();
  });
});
