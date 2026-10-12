import { describe, expect, it } from "vitest";

import { parseRelayConfig } from "../../src/relay/relay.config.js";

const valkey = { VALKEY_NODES: "localhost:7001,localhost:7002" };

describe("parseRelayConfig", () => {
  it("applies defaults for everything except the Valkey nodes", () => {
    expect(parseRelayConfig(valkey)).toEqual({
      nodeEnv: "development",
      logLevel: "info",
      kafkaBrokers: ["localhost:9092"],
      kafkaClientId: "realtime-relay",
      kafkaGroupId: "realtime-relay",
      auctionEventsTopic: "auction.events",
      valkeyNodes: ["localhost:7001", "localhost:7002"],
      partitionConcurrency: 4,
      publishTimeoutMs: 2_000,
      sessionTimeoutMs: 10_000,
      commitIntervalMs: 1_000,
      retryBaseDelayMs: 200,
      retryMaxDelayMs: 10_000,
      shutdownTimeoutMs: 15_000,
    });
  });

  it("trims list entries", () => {
    expect(
      parseRelayConfig({ VALKEY_NODES: " a:1 , b:2 ", KAFKA_BROKERS: "k1:9092, k2:9092" }),
    ).toMatchObject({ valkeyNodes: ["a:1", "b:2"], kafkaBrokers: ["k1:9092", "k2:9092"] });
  });

  it.each([
    ["missing Valkey nodes", {}, "/valkeyNodes"],
    ["a Valkey node without a port", { ...valkey, VALKEY_NODES: "localhost" }, "/valkeyNodes"],
    ["duplicate Valkey nodes", { VALKEY_NODES: "a:1,a:1" }, "must not contain duplicates"],
    [
      "zero partition concurrency",
      { ...valkey, RELAY_PARTITION_CONCURRENCY: "0" },
      "/partitionConcurrency",
    ],
    ["a non-numeric timeout", { ...valkey, RELAY_PUBLISH_TIMEOUT_MS: "soon" }, "/publishTimeoutMs"],
    [
      "a base delay above the maximum",
      { ...valkey, RELAY_RETRY_BASE_DELAY_MS: "5000", RELAY_RETRY_MAX_DELAY_MS: "1000" },
      "must not exceed retryMaxDelayMs",
    ],
    [
      "a shutdown deadline shorter than one publish",
      { ...valkey, RELAY_PUBLISH_TIMEOUT_MS: "5000", RELAY_SHUTDOWN_TIMEOUT_MS: "5500" },
      "at least publishTimeoutMs + 1000",
    ],
    [
      "a session timeout below the broker minimum",
      { ...valkey, RELAY_SESSION_TIMEOUT_MS: "1000" },
      "/sessionTimeoutMs",
    ],
    ["an unknown log level", { ...valkey, LOG_LEVEL: "verbose" }, "/logLevel"],
  ])("rejects %s", (_case, environment, problem) => {
    expect(() => parseRelayConfig(environment)).toThrow(problem);
  });
});
