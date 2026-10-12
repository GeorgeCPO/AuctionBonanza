import { pino } from "pino";

import { PostgresFinalizationRepository } from "./auctions/finalization.repository.js";
import { FinalizationWorker } from "./auctions/finalization.worker.js";
import { createDatabasePool } from "./database.js";
import {
  FINALIZER_IDLE_IN_TRANSACTION_TIMEOUT_MS,
  FINALIZER_LOCK_TIMEOUT_MS,
  FINALIZER_STATEMENT_TIMEOUT_MS,
  parseFinalizerConfig,
} from "./finalizer.config.js";

// Standalone finalizer worker. It shares the auction-service codebase and
// database ownership, but runs, scales, and is configured independently of
// the HTTP API and the outbox publisher (see docs/FINALIZATION.md).
if (import.meta.main) {
  const config = parseFinalizerConfig();
  const logger = pino({ level: config.logLevel, base: { service: "auction-finalizer" } });
  // Each loop holds at most one connection, inside one short transaction.
  const database = createDatabasePool({
    connectionString: config.databaseUrl,
    maxConnections: config.concurrency,
    applicationName: "auction-finalizer",
    keepAlive: true,
    statementTimeoutMs: FINALIZER_STATEMENT_TIMEOUT_MS,
    lockTimeoutMs: FINALIZER_LOCK_TIMEOUT_MS,
    idleInTransactionSessionTimeoutMs: FINALIZER_IDLE_IN_TRANSACTION_TIMEOUT_MS,
    onIdleClientError: (error) => {
      logger.error({ err: error }, "unexpected error from an idle PostgreSQL client");
    },
  });
  const shutdown = new AbortController();

  const requestShutdown = (signal: NodeJS.Signals): void => {
    if (shutdown.signal.aborted) {
      logger.warn({ signal }, "second shutdown signal; exiting without waiting");
      process.exit(1);
    }
    logger.info({ signal }, "shutdown requested; finishing the close in flight");
    shutdown.abort();
    // Last resort. Exiting closes the connection, which rolls back an
    // unfinished close; the auction is claimed again by the next finalizer.
    setTimeout(() => {
      logger.error("graceful shutdown timed out; exiting with the open close rolled back");
      process.exit(1);
    }, config.shutdownTimeoutMs).unref();
  };
  process.on("SIGINT", requestShutdown);
  process.on("SIGTERM", requestShutdown);

  try {
    logger.info(
      { concurrency: config.concurrency, pollIntervalMs: config.pollIntervalMs },
      "finalizer started",
    );
    const repository = new PostgresFinalizationRepository(database);
    const loops = Array.from({ length: config.concurrency }, (_, index) =>
      new FinalizationWorker({
        repository,
        pollIntervalMs: config.pollIntervalMs,
        retryBackoff: { baseDelayMs: config.retryBaseDelayMs, maxDelayMs: config.retryMaxDelayMs },
        logger: logger.child({ loop: index }),
      }).run(shutdown.signal),
    );
    await Promise.all(loops);
  } catch (error) {
    logger.fatal({ err: error }, "finalizer failed");
    process.exitCode = 1;
  } finally {
    await database
      .end()
      .catch((error: unknown) => logger.error({ err: error }, "PostgreSQL pool shutdown failed"));
    logger.info("finalizer stopped");
  }
}
