import { describe, expect, it } from "vitest";

import { parseFinalizerConfig } from "../src/finalizer.config.js";

const environment = { DATABASE_URL: "postgresql://auction:secret@localhost:5432/auction_db" };

describe("finalizer configuration", () => {
  it("applies defaults", () => {
    expect(parseFinalizerConfig(environment)).toMatchObject({
      concurrency: 1,
      pollIntervalMs: 1000,
      retryBaseDelayMs: 500,
      retryMaxDelayMs: 30000,
      auctionRetryMaxDelayMs: 300000,
    });
  });

  it.each([
    ["a missing database URL", {}],
    ["zero loops", { ...environment, FINALIZER_CONCURRENCY: "0" }],
    ["a non-numeric interval", { ...environment, FINALIZER_POLL_INTERVAL_MS: "soon" }],
    [
      "a retry ceiling below its base",
      {
        ...environment,
        FINALIZER_RETRY_BASE_DELAY_MS: "1000",
        FINALIZER_RETRY_MAX_DELAY_MS: "500",
      },
    ],
    [
      "a per-auction cap below the loop's cap",
      { ...environment, FINALIZER_AUCTION_RETRY_MAX_DELAY_MS: "1000" },
    ],
  ])("rejects %s", (_label, candidate) => {
    expect(() => parseFinalizerConfig(candidate)).toThrow("Invalid finalizer configuration");
  });
});
