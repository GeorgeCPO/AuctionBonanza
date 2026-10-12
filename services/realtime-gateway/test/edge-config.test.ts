import { describe, expect, it } from "vitest";

import { parseEdgeConfig } from "../src/edge/edge.config.js";

const valkey = { VALKEY_NODES: "localhost:7001,localhost:7002" };

describe("parseEdgeConfig", () => {
  it("applies defaults for everything except the Valkey nodes", () => {
    expect(parseEdgeConfig(valkey)).toEqual({
      nodeEnv: "development",
      logLevel: "info",
      host: "0.0.0.0",
      port: 3003,
      valkeyNodes: ["localhost:7001", "localhost:7002"],
      valkeyConnectTimeoutMs: 2_000,
      auctionServiceUrl: "http://localhost:3002",
      maxAuctionsPerStream: 50,
      heartbeatIntervalMs: 15_000,
      lingerMs: 30_000,
      liveStateBatchDelayMs: 5,
      liveStateTimeoutMs: 2_000,
      liveStateConcurrency: 4,
      retryBaseDelayMs: 200,
      retryMaxDelayMs: 10_000,
      maxBufferedEventsPerAuction: 1_000,
      maxBufferedBytesPerConnection: 1_048_576,
      shutdownTimeoutMs: 10_000,
    });
  });

  it.each([
    ["missing Valkey nodes", {}, "/valkeyNodes"],
    ["duplicate Valkey nodes", { VALKEY_NODES: "a:1,a:1" }, "must not contain duplicates"],
    [
      "an auction-service URL without a scheme",
      { ...valkey, AUCTION_SERVICE_URL: "localhost:3002" },
      "/auctionServiceUrl",
    ],
    [
      "more auctions per stream than one read covers",
      { ...valkey, EDGE_MAX_AUCTIONS_PER_STREAM: "101" },
      "/maxAuctionsPerStream",
    ],
    [
      "a heartbeat slower than a load balancer's idle timeout",
      { ...valkey, EDGE_HEARTBEAT_INTERVAL_MS: "61000" },
      "/heartbeatIntervalMs",
    ],
    ["a non-numeric linger", { ...valkey, EDGE_LINGER_MS: "soon" }, "/lingerMs"],
    [
      "a base delay above the maximum",
      { ...valkey, EDGE_RETRY_BASE_DELAY_MS: "5000", EDGE_RETRY_MAX_DELAY_MS: "1000" },
      "must not exceed retryMaxDelayMs",
    ],
  ])("rejects %s", (_case, environment, problem) => {
    expect(() => parseEdgeConfig(environment)).toThrow(problem);
  });
});
