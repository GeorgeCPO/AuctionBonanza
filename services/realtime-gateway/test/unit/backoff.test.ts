import { describe, expect, it } from "vitest";

import { backoffDelayMs } from "../../src/shared/backoff.js";

const policy = { baseDelayMs: 100, maxDelayMs: 1_000 };

describe("backoffDelayMs", () => {
  it.each([
    [1, 50, 100],
    [2, 100, 200],
    [3, 200, 400],
    [5, 500, 1_000],
    [60, 500, 1_000],
  ])("keeps attempt %i within [%i, %i]", (attempt, low, high) => {
    expect(backoffDelayMs(policy, attempt, () => 0)).toBe(low);
    expect(backoffDelayMs(policy, attempt, () => 0.999_999)).toBe(high);
  });

  it.each([0, -1, 1.5])("rejects attempt %d", (attempt) => {
    expect(() => backoffDelayMs(policy, attempt)).toThrow(RangeError);
  });
});
