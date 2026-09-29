import { randomUUID } from "node:crypto";

import { Pool } from "pg";
import { pino, type Logger } from "pino";
import { afterAll, afterEach, beforeAll, beforeEach } from "vitest";

import { wrapPool, type Database } from "../../src/database.js";
import { OutboxPublisher } from "../../src/outbox/outbox.publisher.js";
import { KafkaMessageTransport, type MessageTransport } from "../../src/outbox/outbox.transport.js";
import { createIsolatedDatabase, type IsolatedDatabase } from "./isolated-database.js";
import { KafkaTestTopics } from "./kafka.js";
import { AuctionFixtures } from "./outbox-fixtures.js";

export const adminDatabaseUrl = process.env.TEST_DATABASE_URL;
export const kafkaBrokers = process.env.TEST_KAFKA_BROKERS;
export const infrastructureMissing = adminDatabaseUrl === undefined || kafkaBrokers === undefined;

export const PUBLISH_TIMEOUT_MS = 3_000;
// Long enough that a failed event never becomes due by itself during a test.
// Tests make retries due explicitly, which keeps them deterministic.
export const RETRY_DELAY_MS = 60_000;
// Headroom in timing assertions for everything except the send itself: the
// claim and bookkeeping statements, and scheduling on a loaded CI machine.
export const CYCLE_ALLOWANCE_MS = 5_000;

export const silentLogger = pino({ level: "silent" });

export interface PublisherOverrides {
  transport?: MessageTransport;
  database?: Database;
  batchSize?: number;
  logger?: Logger;
}

// Shared setup for the outbox publisher integration tests: a throwaway
// database, a real Kafka transport, and a fresh topic plus users per test.
// Each test starts with an empty outbox so no publisher sees another test's
// events.
export class OutboxPublisherHarness {
  pool!: Pool;
  fixtures!: AuctionFixtures;
  topics!: KafkaTestTopics;
  kafka!: KafkaMessageTransport;
  topic!: string;
  ownerId!: string;
  bidderId!: string;
  private isolatedDatabase: IsolatedDatabase | undefined;
  private readonly publisherPools: Pool[] = [];

  // Registers the harness's lifecycle hooks on the enclosing describe block.
  static register(): OutboxPublisherHarness {
    const harness = new OutboxPublisherHarness();
    beforeAll(async () => harness.setUp());
    beforeEach(async () => harness.startTest());
    afterEach(async () => harness.finishTest());
    afterAll(async () => harness.tearDown());
    return harness;
  }

  publisherDatabase(applicationName = `outbox-publisher-${randomUUID()}`): Database {
    const publisherPool = new Pool({
      connectionString: this.isolatedDatabase!.url,
      application_name: applicationName,
      max: 1,
    });
    // Terminated connections surface through the next query instead.
    publisherPool.on("error", () => {});
    this.publisherPools.push(publisherPool);
    return wrapPool(publisherPool);
  }

  publisher(overrides: PublisherOverrides = {}): OutboxPublisher {
    return new OutboxPublisher({
      database: overrides.database ?? this.publisherDatabase(),
      transport: overrides.transport ?? this.kafka,
      topic: this.topic,
      batchSize: overrides.batchSize ?? 10,
      pollIntervalMs: 20,
      publishTimeoutMs: PUBLISH_TIMEOUT_MS,
      retryBackoff: { baseDelayMs: RETRY_DELAY_MS, maxDelayMs: RETRY_DELAY_MS },
      logger: overrides.logger ?? silentLogger,
      random: () => 0,
    });
  }

  async unpublishedCount(): Promise<number> {
    const result = await this.pool.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM outbox_events WHERE published_at IS NULL",
    );
    return Number(result.rows[0]!.count);
  }

  private async setUp(): Promise<void> {
    this.isolatedDatabase = await createIsolatedDatabase(adminDatabaseUrl!);
    this.pool = new Pool({ connectionString: this.isolatedDatabase.url, max: 4 });
    this.fixtures = new AuctionFixtures(this.pool);
    this.topics = new KafkaTestTopics(kafkaBrokers!);
    await this.topics.connect();
    this.kafka = new KafkaMessageTransport({
      brokers: kafkaBrokers!.split(","),
      clientId: "outbox-publisher-test",
      publishTimeoutMs: PUBLISH_TIMEOUT_MS,
      logger: silentLogger,
    });
    await this.kafka.connect();
  }

  private async startTest(): Promise<void> {
    this.topic = await this.topics.create();
    this.ownerId = await this.fixtures.user();
    this.bidderId = await this.fixtures.user();
  }

  private async finishTest(): Promise<void> {
    await Promise.all(this.publisherPools.splice(0).map(async (pool) => pool.end()));
    // Outbox rows are immutable but deletable.
    await this.pool.query("DELETE FROM outbox_events");
    await this.topics.cleanup();
  }

  private async tearDown(): Promise<void> {
    await this.kafka?.disconnect(1_000);
    await this.topics?.disconnect();
    await this.pool?.end();
    await this.isolatedDatabase?.drop();
  }
}
