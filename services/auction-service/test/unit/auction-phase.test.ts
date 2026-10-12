import { describe, expect, it } from "vitest";

import { derivePhase } from "../../src/auctions/auction.js";

const startsAt = new Date("2030-01-01T10:00:00.000Z");
const endsAt = new Date("2030-01-01T11:00:00.000Z");

describe("derivePhase", () => {
  it("is scheduled strictly before starts_at", () => {
    expect(
      derivePhase(
        { cancelledAt: null, closedAt: null, startsAt, endsAt },
        new Date(startsAt.getTime() - 1),
      ),
    ).toBe("scheduled");
  });

  it("is open at exactly starts_at (the window is half-open)", () => {
    expect(derivePhase({ cancelledAt: null, closedAt: null, startsAt, endsAt }, startsAt)).toBe(
      "open",
    );
  });

  it("is open at the instant just before ends_at", () => {
    expect(
      derivePhase(
        { cancelledAt: null, closedAt: null, startsAt, endsAt },
        new Date(endsAt.getTime() - 1),
      ),
    ).toBe("open");
  });

  it("is awaiting_finalization at exactly ends_at (the window excludes it)", () => {
    expect(derivePhase({ cancelledAt: null, closedAt: null, startsAt, endsAt }, endsAt)).toBe(
      "awaiting_finalization",
    );
  });

  it("is awaiting_finalization after ends_at", () => {
    expect(
      derivePhase(
        { cancelledAt: null, closedAt: null, startsAt, endsAt },
        new Date(endsAt.getTime() + 1),
      ),
    ).toBe("awaiting_finalization");
  });

  it("prefers cancelled over every time-derived phase", () => {
    const cancelledAt = new Date("2030-01-01T10:30:00.000Z");
    expect(
      derivePhase(
        { cancelledAt, closedAt: null, startsAt, endsAt },
        new Date(endsAt.getTime() + 1),
      ),
    ).toBe("cancelled");
    expect(
      derivePhase(
        { cancelledAt, closedAt: null, startsAt, endsAt },
        new Date(startsAt.getTime() - 1),
      ),
    ).toBe("cancelled");
  });

  it("prefers closed over every time-derived phase", () => {
    const closedAt = new Date("2030-01-01T11:00:00.000Z");
    expect(
      derivePhase(
        { cancelledAt: null, closedAt, startsAt, endsAt },
        new Date(startsAt.getTime() - 1),
      ),
    ).toBe("closed");
  });

  it("prefers cancelled over closed when both are set", () => {
    const cancelledAt = new Date("2030-01-01T10:30:00.000Z");
    const closedAt = new Date("2030-01-01T11:00:00.000Z");
    expect(
      derivePhase({ cancelledAt, closedAt, startsAt, endsAt }, new Date(startsAt.getTime() - 1)),
    ).toBe("cancelled");
  });
});
