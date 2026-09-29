import { pino } from "pino";

import { createDatabasePool } from "./database.js";
import { KafkaMessageTransport } from "./outbox/outbox.transport.js";
import { OutboxPublisher } from "./outbox/outbox.publisher.js";
import {
  loadOutboxPublisherConfig,
  LOCK_TIMEOUT_MS,
  SHUTDOWN_CLOSE_MARGIN_MS,
  STATEMENT_TIMEOUT_MS,
} from "./outbox/outbox.config.js";

// Standalone outbox publisher worker. It shares the auction-service codebase
// and database ownership, but runs, scales, and is configured independently
// of the HTTP API process (see docs/OUTBOX_PUBLISHING.md).
if (import.meta.main) {
  const config = loadOutboxPublisherConfig();
  const logger = pino({
    level: config.logLevel,
    base: { service: "auction-outbox-publisher" },
  });
  // A publication cycle keeps one connection checked out, inside a
  // transaction, for the whole Kafka round trip. Each concurrent publication
  // loop therefore needs its own connection, and the pool is sized to exactly
  // that. The idle-in-transaction timeout releases a crashed or stalled
  // publisher's row locks; it is validated to exceed the publish timeout so a
  // healthy but slow publication is never cut off while Kafka might still
  // acknowledge it.
  const database = createDatabasePool({
    connectionString: config.databaseUrl,
    maxConnections: config.concurrency,
    applicationName: config.kafkaClientId,
    keepAlive: true,
    statementTimeoutMs: STATEMENT_TIMEOUT_MS,
    lockTimeoutMs: LOCK_TIMEOUT_MS,
    idleInTransactionSessionTimeoutMs: config.idleInTransactionTimeoutMs,
    onIdleClientError: (error) => {
      logger.error({ err: error }, "unexpected error from an idle PostgreSQL client");
    },
  });
  const transport = new KafkaMessageTransport({
    brokers: config.kafkaBrokers,
    clientId: config.kafkaClientId,
    publishTimeoutMs: config.publishTimeoutMs,
    logger,
  });
  const shutdown = new AbortController();
  let shutdownDeadline: number | undefined;

  const requestShutdown = (signal: NodeJS.Signals): void => {
    if (shutdown.signal.aborted) {
      logger.warn({ signal }, "second shutdown signal; exiting without waiting");
      process.exit(1);
    }
    logger.info(
      { signal, shutdownTimeoutMs: config.shutdownTimeoutMs },
      "shutdown requested; finishing in-flight publication cycles",
    );
    shutdownDeadline = Date.now() + config.shutdownTimeoutMs;
    shutdown.abort();
    // Last resort. Exiting closes the database connections, so uncommitted
    // claims roll back and their events are published again later.
    setTimeout(() => {
      logger.error("graceful shutdown timed out; exiting with uncommitted claims rolled back");
      process.exit(1);
    }, config.shutdownTimeoutMs).unref();
  };
  process.on("SIGINT", requestShutdown);
  process.on("SIGTERM", requestShutdown);

  try {
    await transport.connect();
    logger.info(
      {
        brokers: config.kafkaBrokers,
        topic: config.auctionEventsTopic,
        concurrency: config.concurrency,
        batchSize: config.batchSize,
        publishTimeoutMs: config.publishTimeoutMs,
      },
      "outbox publisher started",
    );

    const loops = Array.from({ length: config.concurrency }, (_, index) =>
      new OutboxPublisher({
        database,
        transport,
        topic: config.auctionEventsTopic,
        batchSize: config.batchSize,
        pollIntervalMs: config.pollIntervalMs,
        publishTimeoutMs: config.publishTimeoutMs,
        retryBackoff: { baseDelayMs: config.retryBaseDelayMs, maxDelayMs: config.retryMaxDelayMs },
        logger: logger.child({ loop: index }),
      }).run(shutdown.signal),
    );
    await Promise.all(loops);
  } catch (error) {
    logger.fatal({ err: error }, "outbox publisher failed");
    process.exitCode = 1;
  } finally {
    // Every publication outcome is committed by now, so the flush only
    // spends what is left of the shutdown deadline. Anything it abandons is a
    // send already recorded as failed, which will be retried.
    const flushTimeoutMs =
      shutdownDeadline === undefined
        ? config.publishTimeoutMs
        : Math.max(0, shutdownDeadline - Date.now() - SHUTDOWN_CLOSE_MARGIN_MS);
    await transport
      .disconnect(flushTimeoutMs)
      .catch((error: unknown) => logger.error({ err: error }, "Kafka producer disconnect failed"));
    await database
      .end()
      .catch((error: unknown) => logger.error({ err: error }, "PostgreSQL pool shutdown failed"));
    logger.info("outbox publisher stopped");
  }
}
