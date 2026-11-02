import { describe, expect, it } from "vitest";

import { derivePhase, formatCountdown } from "../../src/live/phase.js";

const input = {
  startsAt: "2026-01-01T00:00:00.000Z",
  endsAt: "2026-01-01T01:00:00.000Z",
  cancelledAt: null,
  closedAt: null,
};

describe("derivePhase", () => {
  it("follows the dates", () => {
    expect(derivePhase(input, Date.parse("2025-12-31T23:59:59.000Z"))).toBe("scheduled");
    expect(derivePhase(input, Date.parse("2026-01-01T00:30:00.000Z"))).toBe("open");
    expect(derivePhase(input, Date.parse("2026-01-01T01:00:00.000Z"))).toBe("ended");
  });

  it("prefers cancelled, then closed, over the dates", () => {
    const now = Date.parse("2026-01-01T00:30:00.000Z");

    expect(derivePhase({ ...input, closedAt: "2026-01-01T01:00:05.000Z" }, now)).toBe("closed");
    expect(
      derivePhase(
        { ...input, cancelledAt: "2026-01-01T00:10:00.000Z", closedAt: "2026-01-01T01:00:05.000Z" },
        now,
      ),
    ).toBe("cancelled");
  });
});

describe("formatCountdown", () => {
  const target = "2026-01-01T01:00:00.000Z";

  it("formats hours, minutes and seconds", () => {
    expect(formatCountdown(target, Date.parse("2026-01-01T00:00:00.000Z"))).toBe("1h 00m 00s");
    expect(formatCountdown(target, Date.parse("2026-01-01T00:57:57.000Z"))).toBe("2m 03s");
  });

  it("is undefined once the time has passed", () => {
    expect(formatCountdown(target, Date.parse(target))).toBeUndefined();
    expect(formatCountdown(target, Date.parse("2026-01-01T02:00:00.000Z"))).toBeUndefined();
  });
});
