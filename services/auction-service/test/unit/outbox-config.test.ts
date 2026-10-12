import { describe, expect, it } from "vitest";

import { parseOutboxPublisherConfig } from "../../src/outbox/outbox.config.js";

const databaseUrl = "postgresql://auction:auction_local@localhost:5432/auction_db";

describe("parseOutboxPublisherConfig", () => {
  it("uses local-safe defaults", () => {
    expect(parseOutboxPublisherConfig({ DATABASE_URL: databaseUrl })).toEqual({
      nodeEnv: "development",
      logLevel: "info",
      databaseUrl,
      kafkaBrokers: ["localhost:9092"],
      kafkaClientId: "auction-outbox-publisher",
      auctionEventsTopic: "auction.events",
      concurrency: 1,
      batchSize: 50,
      pollIntervalMs: 250,
      publishTimeoutMs: 10_000,
      retryBaseDelayMs: 500,
      retryMaxDelayMs: 60_000,
      idleInTransactionTimeoutMs: 30_000,
      shutdownTimeoutMs: 30_000,
    });
  });

  it("parses explicit settings and trims the broker list", () => {
    expect(
      parseOutboxPublisherConfig({
        NODE_ENV: "production",
        LOG_LEVEL: "warn",
        DATABASE_URL: databaseUrl,
        KAFKA_BROKERS: "kafka-1:9092, kafka-2:9092",
        KAFKA_CLIENT_ID: "publisher-a",
        AUCTION_EVENTS_TOPIC: "auction.events.test",
        OUTBOX_PUBLISHER_CONCURRENCY: "4",
        OUTBOX_BATCH_SIZE: "100",
        OUTBOX_POLL_INTERVAL_MS: "50",
        OUTBOX_PUBLISH_TIMEOUT_MS: "5000",
        OUTBOX_RETRY_BASE_DELAY_MS: "100",
        OUTBOX_RETRY_MAX_DELAY_MS: "1000",
        OUTBOX_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS: "10000",
        OUTBOX_SHUTDOWN_TIMEOUT_MS: "12000",
      }),
    ).toMatchObject({
      nodeEnv: "production",
      logLevel: "warn",
      kafkaBrokers: ["kafka-1:9092", "kafka-2:9092"],
      kafkaClientId: "publisher-a",
      auctionEventsTopic: "auction.events.test",
      concurrency: 4,
      batchSize: 100,
      pollIntervalMs: 50,
      publishTimeoutMs: 5_000,
      retryBaseDelayMs: 100,
      retryMaxDelayMs: 1_000,
      idleInTransactionTimeoutMs: 10_000,
      shutdownTimeoutMs: 12_000,
    });
  });

  it.each([
    [{ DATABASE_URL: undefined }, "/databaseUrl"],
    [{ KAFKA_BROKERS: "" }, "/kafkaBrokers/0"],
    [{ KAFKA_BROKERS: "localhost" }, "/kafkaBrokers/0"],
    [{ AUCTION_EVENTS_TOPIC: "auction events" }, "/auctionEventsTopic"],
    [{ OUTBOX_BATCH_SIZE: "0" }, "/batchSize"],
    [{ OUTBOX_BATCH_SIZE: "1.5" }, "/batchSize"],
    [{ OUTBOX_PUBLISHER_CONCURRENCY: "abc" }, "/concurrency"],
    [{ OUTBOX_PUBLISH_TIMEOUT_MS: "500" }, "/publishTimeoutMs"],
    [{ LOG_LEVEL: "verbose" }, "/logLevel"],
  ])("rejects invalid values: %o", (overrides, path) => {
    expect(() => parseOutboxPublisherConfig({ DATABASE_URL: databaseUrl, ...overrides })).toThrow(
      path,
    );
  });

  it("rejects settings that are individually valid but inconsistent", () => {
    expect(() =>
      parseOutboxPublisherConfig({
        DATABASE_URL: databaseUrl,
        OUTBOX_RETRY_BASE_DELAY_MS: "2000",
        OUTBOX_RETRY_MAX_DELAY_MS: "1000",
      }),
    ).toThrow("/retryBaseDelayMs: must not exceed retryMaxDelayMs");

    // PostgreSQL must never end the claiming transaction while Kafka might
    // still acknowledge the publication.
    expect(() =>
      parseOutboxPublisherConfig({
        DATABASE_URL: databaseUrl,
        OUTBOX_PUBLISH_TIMEOUT_MS: "10000",
        OUTBOX_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS: "14999",
      }),
    ).toThrow("/idleInTransactionTimeoutMs: must be at least publishTimeoutMs + 5000");

    // An in-flight cycle may need the publish timeout, the publisher's 2 s
    // timer grace, and 5 s for its bookkeeping and COMMIT before it is safe
    // to stop.
    expect(() =>
      parseOutboxPublisherConfig({
        DATABASE_URL: databaseUrl,
        OUTBOX_PUBLISH_TIMEOUT_MS: "10000",
        OUTBOX_SHUTDOWN_TIMEOUT_MS: "16999",
      }),
    ).toThrow(
      "/shutdownTimeoutMs: must be at least publishTimeoutMs + 7000 to let an in-flight publication cycle commit",
    );
    expect(
      parseOutboxPublisherConfig({
        DATABASE_URL: databaseUrl,
        OUTBOX_PUBLISH_TIMEOUT_MS: "10000",
        OUTBOX_SHUTDOWN_TIMEOUT_MS: "17000",
      }).shutdownTimeoutMs,
    ).toBe(17_000);

    expect(() =>
      parseOutboxPublisherConfig({
        DATABASE_URL: databaseUrl,
        KAFKA_BROKERS: "localhost:9092,localhost:9092",
      }),
    ).toThrow("/kafkaBrokers: must not contain duplicates");
  });
});
