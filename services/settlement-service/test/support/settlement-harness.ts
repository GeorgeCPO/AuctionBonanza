import { randomUUID } from "node:crypto";

import type { AuctionClosedV1 } from "@auction-bonanza/contracts";
import { pino } from "pino";

import { KafkaDeadLetterPublisher } from "../../src/dead-letter.publisher.js";
import type { DeadLetterPublisher, DeadLetterRecord } from "../../src/dead-letter.types.js";
import { SettlementConsumer } from "../../src/settlement.consumer.js";
import { SettlementHandler } from "../../src/settlement.handler.js";
import { PostgresSettlementRepository } from "../../src/settlement.repository.js";
import type {
  SettlementOutcome,
  SettlementRepository,
} from "../../src/settlement.repository.types.js";
import type { SettlementRun, StartSettlementOptions } from "./settlement-harness.types.js";

// Wraps the real repository. `failWhile` returns the error a settle should fail
// with (before touching the database), or undefined to let it through. Every
// attempt is recorded, successful or not.
export class FlakyRepository implements SettlementRepository {
  readonly attempts: AuctionClosedV1[] = [];
  failWhile: (event: AuctionClosedV1) => Error | undefined = () => undefined;

  constructor(private readonly inner: SettlementRepository) {}

  async settle(event: AuctionClosedV1): Promise<SettlementOutcome> {
    this.attempts.push(event);
    const failure = this.failWhile(event);
    if (failure !== undefined) {
      throw failure;
    }
    return this.inner.settle(event);
  }
}

// Wraps the real dead-letter publisher the same way: while `failing` is set,
// every publish is refused before reaching the broker.
export class FlakyDeadLetters implements DeadLetterPublisher {
  readonly attempts: DeadLetterRecord[] = [];
  failing = false;

  constructor(private readonly inner: DeadLetterPublisher) {}

  async publish(record: DeadLetterRecord): Promise<void> {
    this.attempts.push(record);
    if (this.failing) {
      throw new Error("scripted dead-letter failure");
    }
    await this.inner.publish(record);
  }
}

// Starts the real consumer, handler, repository and dead-letter producer
// against real Kafka and PostgreSQL, with short intervals so retries and
// commits happen quickly in a test. The repository and the dead-letter
// publisher are wrapped so a test can fail them on demand.
export async function startSettlement(options: StartSettlementOptions): Promise<SettlementRun> {
  const logger = options.logger ?? pino({ level: "silent" });
  const realDeadLetters = new KafkaDeadLetterPublisher({
    brokers: [options.brokers],
    clientId: `settlement-test-dlq-${randomUUID()}`,
    topic: options.deadLetterTopic,
    sendTimeoutMs: 5_000,
    logger,
  });
  await realDeadLetters.connect();
  const repository = new FlakyRepository(new PostgresSettlementRepository(options.database));
  const deadLetters = new FlakyDeadLetters(realDeadLetters);
  const consumer = new SettlementConsumer({
    brokers: [options.brokers],
    clientId: `settlement-test-${randomUUID()}`,
    groupId: options.groupId,
    topic: options.topic,
    handler: new SettlementHandler({
      repository,
      deadLetters,
      consumerGroup: options.groupId,
      logger,
    }),
    partitionConcurrency: 4,
    commitIntervalMs: 100,
    sessionTimeoutMs: 6_000,
    retryBackoff: { baseDelayMs: 50, maxDelayMs: 200 },
    logger,
  });
  try {
    await consumer.start();
  } catch (error) {
    // Nothing returned to the caller can stop what was already started.
    await consumer.stop().catch(() => {});
    await realDeadLetters.disconnect(5_000);
    throw error;
  }

  let stopped = false;
  return {
    repository,
    deadLetters,
    stop: async () => {
      if (stopped) {
        return;
      }
      stopped = true;
      await consumer.stop();
      await realDeadLetters.disconnect(5_000);
    },
  };
}
