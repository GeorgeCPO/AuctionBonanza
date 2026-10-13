import { pino } from "pino";

import { KafkaDeadLetterPublisher } from "./dead-letter.publisher.js";
import { SettlementConsumer } from "./settlement.consumer.js";
import {
  loadSettlementConfig,
  SETTLEMENT_IDLE_IN_TRANSACTION_TIMEOUT_MS,
  SETTLEMENT_LOCK_TIMEOUT_MS,
  SETTLEMENT_STATEMENT_TIMEOUT_MS,
} from "./settlement.config.js";
import { SettlementHandler } from "./settlement.handler.js";
import { SETTLEMENT_LOG_MESSAGES } from "./settlement.log-messages.js";
import { PostgresSettlementRepository } from "./settlement.repository.js";
import { createDatabasePool } from "./shared/database.js";

// The settlement worker: consumes AuctionClosed.v1 from auction.events and
// creates the winner's order (see docs/SETTLEMENT.md). It runs and scales
// separately from auction-service; consumers of one group share the topic's
// partitions, up to one worker per partition.
if (import.meta.main) {
  const config = loadSettlementConfig();
  const logger = pino({ level: config.logLevel, base: { service: "settlement-worker" } });
  // Each message in flight holds at most one connection, inside one short
  // transaction, so the pool is as large as the number of messages in flight.
  const database = createDatabasePool({
    connectionString: config.databaseUrl,
    maxConnections: config.partitionConcurrency,
    applicationName: "settlement-worker",
    keepAlive: true,
    statementTimeoutMs: SETTLEMENT_STATEMENT_TIMEOUT_MS,
    lockTimeoutMs: SETTLEMENT_LOCK_TIMEOUT_MS,
    idleInTransactionSessionTimeoutMs: SETTLEMENT_IDLE_IN_TRANSACTION_TIMEOUT_MS,
    onIdleClientError: (error) => {
      logger.error({ err: error }, "unexpected error from an idle PostgreSQL client");
    },
  });
  const deadLetters = new KafkaDeadLetterPublisher({
    brokers: config.kafkaBrokers,
    clientId: `${config.kafkaClientId}-dead-letter`,
    topic: config.deadLetterTopic,
    sendTimeoutMs: config.deadLetterTimeoutMs,
    logger,
  });
  const consumer = new SettlementConsumer({
    brokers: config.kafkaBrokers,
    clientId: config.kafkaClientId,
    groupId: config.kafkaGroupId,
    topic: config.auctionEventsTopic,
    handler: new SettlementHandler({
      repository: new PostgresSettlementRepository(database),
      deadLetters,
      consumerGroup: config.kafkaGroupId,
      logger,
    }),
    partitionConcurrency: config.partitionConcurrency,
    commitIntervalMs: config.commitIntervalMs,
    sessionTimeoutMs: config.sessionTimeoutMs,
    retryBackoff: { baseDelayMs: config.retryBaseDelayMs, maxDelayMs: config.retryMaxDelayMs },
    logger,
  });

  let stopping: Promise<void> | undefined;
  const shutdown = async (): Promise<void> => {
    // The consumer first: it waits for in-flight messages, which still need
    // the database and the dead-letter producer, and commits their offsets.
    try {
      await consumer.stop();
    } catch (error) {
      logger.error({ err: error }, "Kafka consumer shutdown failed");
      process.exitCode = 1;
    }
    await deadLetters
      .disconnect(config.deadLetterTimeoutMs)
      .catch((error: unknown) =>
        logger.error({ err: error }, "dead-letter producer shutdown failed"),
      );
    await database
      .end()
      .catch((error: unknown) => logger.error({ err: error }, "PostgreSQL pool shutdown failed"));
    logger.info(SETTLEMENT_LOG_MESSAGES.stopped);
  };

  const requestShutdown = (signal: NodeJS.Signals): void => {
    if (stopping !== undefined) {
      logger.warn({ signal }, "second shutdown signal; exiting without waiting");
      process.exit(1);
    }
    logger.info(
      { signal, shutdownTimeoutMs: config.shutdownTimeoutMs },
      SETTLEMENT_LOG_MESSAGES.shutdownRequested,
    );
    // Last resort. A transaction still open then is rolled back when the
    // process exits, and an offset not yet committed delivers its message
    // again, which settlement recognises as a repeat.
    setTimeout(() => {
      logger.error("graceful shutdown timed out; exiting");
      process.exit(1);
    }, config.shutdownTimeoutMs).unref();
    stopping = shutdown();
  };
  process.on("SIGINT", requestShutdown);
  process.on("SIGTERM", requestShutdown);

  try {
    // The dead-letter producer first: a worker that consumed without it could
    // read a bad record and have nowhere to send it.
    await deadLetters.connect();
    await consumer.start();
    logger.info(
      {
        brokers: config.kafkaBrokers,
        topic: config.auctionEventsTopic,
        deadLetterTopic: config.deadLetterTopic,
        groupId: config.kafkaGroupId,
        partitionConcurrency: config.partitionConcurrency,
      },
      SETTLEMENT_LOG_MESSAGES.started,
    );
  } catch (error) {
    logger.fatal({ err: error }, SETTLEMENT_LOG_MESSAGES.failedToStart);
    process.exitCode = 1;
    await shutdown();
  }
}
