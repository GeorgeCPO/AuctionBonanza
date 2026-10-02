import { pino } from "pino";

import { ValkeyBusPublisher } from "./bus/valkey-bus-publisher.js";
import { EventRelay } from "./relay/relay.js";
import { loadRelayConfig } from "./relay/relay.config.js";

// The realtime gateway's relay process: consumes auction.events and publishes
// each event to its auction's Valkey channel. It runs and scales separately
// from the SSE edges (see docs/REALTIME_GATEWAY.md).
if (import.meta.main) {
  const config = loadRelayConfig();
  const logger = pino({ level: config.logLevel, base: { service: "realtime-relay" } });
  const publisher = new ValkeyBusPublisher({
    nodes: config.valkeyNodes,
    commandTimeoutMs: config.publishTimeoutMs,
    logger,
  });
  const relay = new EventRelay({
    brokers: config.kafkaBrokers,
    clientId: config.kafkaClientId,
    groupId: config.kafkaGroupId,
    topic: config.auctionEventsTopic,
    publisher,
    partitionConcurrency: config.partitionConcurrency,
    commitIntervalMs: config.commitIntervalMs,
    sessionTimeoutMs: config.sessionTimeoutMs,
    retryBackoff: { baseDelayMs: config.retryBaseDelayMs, maxDelayMs: config.retryMaxDelayMs },
    logger,
  });

  let stopping: Promise<void> | undefined;
  const shutdown = async (): Promise<void> => {
    try {
      await relay.stop();
    } catch (error) {
      logger.error({ err: error }, "Kafka consumer shutdown failed");
      process.exitCode = 1;
    }
    await publisher
      .close()
      .catch((error: unknown) => logger.error({ err: error }, "Valkey client shutdown failed"));
    logger.info("relay stopped");
  };

  const requestShutdown = (signal: NodeJS.Signals): void => {
    if (stopping !== undefined) {
      logger.warn({ signal }, "second shutdown signal; exiting without waiting");
      process.exit(1);
    }
    logger.info(
      { signal, shutdownTimeoutMs: config.shutdownTimeoutMs },
      "shutdown requested; finishing in-flight messages",
    );
    // Last resort. Offsets of unfinished messages are not committed, so the
    // next owner of their partitions publishes them again.
    setTimeout(() => {
      logger.error("graceful shutdown timed out; exiting");
      process.exit(1);
    }, config.shutdownTimeoutMs).unref();
    stopping = shutdown();
  };
  process.on("SIGINT", requestShutdown);
  process.on("SIGTERM", requestShutdown);

  try {
    // Valkey first: a relay that consumed without a bus would only pause.
    await publisher.connect();
    await relay.start();
    logger.info(
      {
        brokers: config.kafkaBrokers,
        valkeyNodes: config.valkeyNodes,
        topic: config.auctionEventsTopic,
        groupId: config.kafkaGroupId,
        partitionConcurrency: config.partitionConcurrency,
      },
      "relay started",
    );
  } catch (error) {
    logger.fatal({ err: error }, "relay failed to start");
    process.exitCode = 1;
    await shutdown();
  }
}
