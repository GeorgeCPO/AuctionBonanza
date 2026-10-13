import { describe, expect, it } from "vitest";

import { parseSettlementConfig } from "../../src/settlement.config.js";

const required = { DATABASE_URL: "postgresql://auction:secret@localhost:5432/auction_db" };

describe("parseSettlementConfig", () => {
  it("fills in the defaults", () => {
    expect(parseSettlementConfig(required)).toEqual({
      nodeEnv: "development",
      logLevel: "info",
      databaseUrl: required.DATABASE_URL,
      kafkaBrokers: ["localhost:9092"],
      kafkaClientId: "settlement-worker",
      kafkaGroupId: "settlement-worker",
      auctionEventsTopic: "auction.events",
      deadLetterTopic: "auction.events.dlq",
      partitionConcurrency: 4,
      deadLetterTimeoutMs: 5_000,
      sessionTimeoutMs: 10_000,
      commitIntervalMs: 1_000,
      retryBaseDelayMs: 200,
      retryMaxDelayMs: 10_000,
      shutdownTimeoutMs: 15_000,
    });
  });

  it("reads every setting from its environment variable", () => {
    const config = parseSettlementConfig({
      ...required,
      NODE_ENV: "production",
      LOG_LEVEL: "debug",
      KAFKA_BROKERS: "kafka-1:9092, kafka-2:9092",
      KAFKA_CLIENT_ID: "settlement-2",
      KAFKA_GROUP_ID: "settlement-blue",
      AUCTION_EVENTS_TOPIC: "events",
      SETTLEMENT_DEAD_LETTER_TOPIC: "events.dead",
      SETTLEMENT_PARTITION_CONCURRENCY: "8",
      SETTLEMENT_DEAD_LETTER_TIMEOUT_MS: "2000",
      SETTLEMENT_SESSION_TIMEOUT_MS: "20000",
      SETTLEMENT_COMMIT_INTERVAL_MS: "500",
      SETTLEMENT_RETRY_BASE_DELAY_MS: "100",
      SETTLEMENT_RETRY_MAX_DELAY_MS: "5000",
      SETTLEMENT_SHUTDOWN_TIMEOUT_MS: "30000",
    });

    expect(config).toMatchObject({
      nodeEnv: "production",
      logLevel: "debug",
      kafkaBrokers: ["kafka-1:9092", "kafka-2:9092"],
      kafkaClientId: "settlement-2",
      kafkaGroupId: "settlement-blue",
      auctionEventsTopic: "events",
      deadLetterTopic: "events.dead",
      partitionConcurrency: 8,
      deadLetterTimeoutMs: 2_000,
      sessionTimeoutMs: 20_000,
      commitIntervalMs: 500,
      retryBaseDelayMs: 100,
      retryMaxDelayMs: 5_000,
      shutdownTimeoutMs: 30_000,
    });
  });

  it.each([
    ["a missing database", { DATABASE_URL: undefined }, "/databaseUrl"],
    ["a database that is not PostgreSQL", { DATABASE_URL: "mysql://x" }, "/databaseUrl"],
    ["an unknown log level", { LOG_LEVEL: "loud" }, "/logLevel"],
    ["a broker without a port", { KAFKA_BROKERS: "kafka" }, "/kafkaBrokers"],
    ["a group name with a space", { KAFKA_GROUP_ID: "a b" }, "/kafkaGroupId"],
    ["no concurrency", { SETTLEMENT_PARTITION_CONCURRENCY: "0" }, "/partitionConcurrency"],
    [
      "fractional concurrency",
      { SETTLEMENT_PARTITION_CONCURRENCY: "1.5" },
      "/partitionConcurrency",
    ],
    [
      "a session timeout below the broker's floor",
      { SETTLEMENT_SESSION_TIMEOUT_MS: "5999" },
      "/sessionTimeoutMs",
    ],
    [
      "a non-numeric timeout",
      { SETTLEMENT_DEAD_LETTER_TIMEOUT_MS: "soon" },
      "/deadLetterTimeoutMs",
    ],
    // librdkafka rejects the connection timeout derived from it below 1000 ms,
    // which would otherwise fail the producer at startup naming no setting.
    [
      "a dead-letter timeout below librdkafka's floor",
      { SETTLEMENT_DEAD_LETTER_TIMEOUT_MS: "999" },
      "/deadLetterTimeoutMs",
    ],
  ])("rejects %s", (_description, overrides, path) => {
    expect(() => parseSettlementConfig({ ...required, ...overrides })).toThrow(
      new RegExp(`^Invalid settlement configuration: .*${path}`),
    );
  });

  it("rejects a retry base delay above its cap", () => {
    expect(() =>
      parseSettlementConfig({
        ...required,
        SETTLEMENT_RETRY_BASE_DELAY_MS: "5000",
        SETTLEMENT_RETRY_MAX_DELAY_MS: "1000",
      }),
    ).toThrow("/retryBaseDelayMs: must not exceed retryMaxDelayMs");
  });

  it("rejects a shutdown timeout that cannot cover a dead-letter send", () => {
    expect(() =>
      parseSettlementConfig({
        ...required,
        SETTLEMENT_DEAD_LETTER_TIMEOUT_MS: "5000",
        SETTLEMENT_SHUTDOWN_TIMEOUT_MS: "5999",
      }),
    ).toThrow("/shutdownTimeoutMs: must be at least deadLetterTimeoutMs + 1000");
  });

  it("accepts the smallest shutdown timeout that can", () => {
    expect(() =>
      parseSettlementConfig({
        ...required,
        SETTLEMENT_DEAD_LETTER_TIMEOUT_MS: "5000",
        SETTLEMENT_SHUTDOWN_TIMEOUT_MS: "6000",
      }),
    ).not.toThrow();
  });

  it("rejects a dead-letter topic that is the input topic", () => {
    expect(() =>
      parseSettlementConfig({
        ...required,
        AUCTION_EVENTS_TOPIC: "auction.events",
        SETTLEMENT_DEAD_LETTER_TOPIC: "auction.events",
      }),
    ).toThrow("/deadLetterTopic: must differ from auctionEventsTopic");
  });

  it("rejects duplicate brokers", () => {
    expect(() =>
      parseSettlementConfig({ ...required, KAFKA_BROKERS: "kafka:9092,kafka:9092" }),
    ).toThrow("/kafkaBrokers: must not contain duplicates");
  });
});
