import { describe, expect, it } from "vitest";

import { backoffDelayMs } from "../../src/shared/backoff.js";

const policy = { baseDelayMs: 100, maxDelayMs: 1_000 };

// Math.random() returns [0, 1), so the largest draw it can make is just
// below 1; this stands in for it and lands on the ceiling.
const highestDraw = () => 0.999_999;

describe("backoffDelayMs", () => {
  it("doubles the ceiling each attempt until the cap", () => {
    const top = (attempt: number) => backoffDelayMs(policy, attempt, highestDraw);

    expect([1, 2, 3, 4, 5, 6].map(top)).toEqual([100, 200, 400, 800, 1_000, 1_000]);
  });

  it("keeps half of the ceiling fixed and randomises the other half", () => {
    expect(backoffDelayMs(policy, 3, () => 0)).toBe(200);
    expect(backoffDelayMs(policy, 3, () => 0.5)).toBe(300);
    expect(backoffDelayMs(policy, 3, highestDraw)).toBe(400);
  });

  it("does not overflow for a very large attempt count", () => {
    expect(backoffDelayMs(policy, 10_000, highestDraw)).toBe(1_000);
  });

  it.each([0, -1, 1.5, Number.NaN])("rejects attempt %s", (attempt) => {
    expect(() => backoffDelayMs(policy, attempt)).toThrow(RangeError);
  });
});
