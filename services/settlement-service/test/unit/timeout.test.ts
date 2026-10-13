import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { withTimeout } from "../../src/shared/timeout.js";

describe("withTimeout", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns the result of an operation that finishes in time", async () => {
    const result = withTimeout(Promise.resolve("sent"), 1_000, "send");

    await expect(result).resolves.toBe("sent");
    expect(vi.getTimerCount()).toBe(0);
  });

  it("passes on the failure of an operation that fails in time", async () => {
    const failure = new Error("broker unavailable");

    await expect(withTimeout(Promise.reject(failure), 1_000, "send")).rejects.toBe(failure);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("gives up on an operation that takes too long, naming it", async () => {
    // Capture the outcome at once, so the rejection is handled the moment the
    // fake clock raises it.
    const outcome = withTimeout(new Promise<never>(() => {}), 1_000, "dead-letter send").catch(
      (error: unknown) => error,
    );

    await vi.advanceTimersByTimeAsync(1_000);

    expect(await outcome).toEqual(new Error("dead-letter send timed out after 1000ms"));
  });

  it("does not give up early", async () => {
    let done = false;
    const result = withTimeout(
      new Promise<string>((resolve) => setTimeout(() => resolve("late but in time"), 900)),
      1_000,
      "send",
    ).then((value) => {
      done = true;
      return value;
    });

    await vi.advanceTimersByTimeAsync(899);
    expect(done).toBe(false);
    await vi.advanceTimersByTimeAsync(1);

    await expect(result).resolves.toBe("late but in time");
  });

  it("does not leave an unhandled rejection when the operation fails after the timeout", async () => {
    let failLater: (error: Error) => void = () => {};
    const operation = new Promise<never>((_resolve, reject) => {
      failLater = reject;
    });
    const outcome = withTimeout(operation, 1_000, "send").catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(await outcome).toEqual(new Error("send timed out after 1000ms"));

    // Vitest fails the run on an unhandled rejection, so reaching the end of
    // this test is the assertion.
    failLater(new Error("failed after the caller gave up"));
    await vi.advanceTimersByTimeAsync(0);
  });
});
