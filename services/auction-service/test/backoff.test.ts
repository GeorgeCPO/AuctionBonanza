import { describe, expect, it } from "vitest";

import { backoffDelayMs } from "../src/shared/backoff.js";

const policy = { baseDelayMs: 100, maxDelayMs: 1_000 };

describe("backoffDelayMs", () => {
  it("doubles the ceiling per attempt and draws from its upper half", () => {
    expect([1, 2, 3, 4].map((attempt) => backoffDelayMs(policy, attempt, () => 0))).toEqual([
      50, 100, 200, 400,
    ]);
    expect([1, 2, 3, 4].map((attempt) => backoffDelayMs(policy, attempt, () => 0.999_999))).toEqual(
      [100, 200, 400, 800],
    );
  });

  it("never exceeds the configured maximum, even for very large attempt counts", () => {
    for (const attempt of [5, 10, 31, 1_000, Number.MAX_SAFE_INTEGER]) {
      const delay = backoffDelayMs(policy, attempt, () => 0.999_999);
      expect(delay).toBe(1_000);
      expect(backoffDelayMs(policy, attempt, () => 0)).toBe(500);
    }
  });

  it("rejects attempts that are not positive integers", () => {
    for (const attempt of [0, -1, 1.5, Number.NaN]) {
      expect(() => backoffDelayMs(policy, attempt)).toThrow(RangeError);
    }
  });
});
