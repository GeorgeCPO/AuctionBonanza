import { pino } from "pino";

import { ValkeyBusSubscriber } from "./bus/valkey-bus-subscriber.js";
import { AuctionRegistry } from "./edge/auction-registry.js";
import { loadEdgeConfig } from "./edge/edge.config.js";
import { EdgeServer } from "./edge/edge-server.js";
import { LiveStateBatcher, LiveStateClient } from "./edge/live-states.js";

// The realtime gateway's SSE edge process: serves GET /v1/stream, subscribes
// to the Valkey channels of the auctions its clients watch, and reads live
// states from auction-service. It runs and scales separately from the relay
// (see docs/REALTIME_GATEWAY.md).
if (import.meta.main) {
  const config = loadEdgeConfig();
  const logger = pino({ level: config.logLevel, base: { service: "realtime-edge" } });
  const bus = new ValkeyBusSubscriber({
    nodes: config.valkeyNodes,
    clientName: "realtime-edge",
    connectTimeoutMs: config.valkeyConnectTimeoutMs,
    logger,
  });
  const registry = new AuctionRegistry({
    bus,
    liveStates: new LiveStateBatcher({
      client: new LiveStateClient({
        auctionServiceUrl: config.auctionServiceUrl,
        timeoutMs: config.liveStateTimeoutMs,
      }),
      batchDelayMs: config.liveStateBatchDelayMs,
      maxConcurrentRequests: config.liveStateConcurrency,
    }),
    lingerMs: config.lingerMs,
    maxBufferedEvents: config.maxBufferedEventsPerAuction,
    retryBackoff: { baseDelayMs: config.retryBaseDelayMs, maxDelayMs: config.retryMaxDelayMs },
    logger,
  });
  const server = new EdgeServer({
    registry,
    maxAuctionsPerStream: config.maxAuctionsPerStream,
    heartbeatIntervalMs: config.heartbeatIntervalMs,
    maxBufferedBytesPerConnection: config.maxBufferedBytesPerConnection,
    logger,
  });

  let stopping: Promise<void> | undefined;
  let starting: Promise<void> | undefined;

  const start = async (): Promise<void> => {
    // Valkey first: an edge that accepted clients without a bus could only
    // leave them waiting.
    await bus.connect();
    // A shutdown requested while connecting must not be followed by
    // accepting clients.
    if (stopping !== undefined) return;
    const address = await server.listen(config.host, config.port);
    logger.info(
      {
        host: address.address,
        port: address.port,
        valkeyNodes: config.valkeyNodes,
        auctionServiceUrl: config.auctionServiceUrl,
      },
      "edge started",
    );
  };

  const shutdown = async (): Promise<void> => {
    // Let startup finish first, so that whatever it opened is closed below.
    await starting?.catch(() => {});
    try {
      await server.close();
    } catch (error) {
      logger.error({ err: error }, "HTTP server shutdown failed");
      process.exitCode = 1;
    }
    registry.close();
    await bus
      .close()
      .catch((error: unknown) => logger.error({ err: error }, "Valkey client shutdown failed"));
    logger.info("edge stopped");
  };

  const requestShutdown = (signal: NodeJS.Signals): void => {
    if (stopping !== undefined) {
      logger.warn({ signal }, "second shutdown signal; exiting without waiting");
      process.exit(1);
    }
    logger.info({ signal, connections: server.connectionCount }, "shutdown requested");
    setTimeout(() => {
      logger.error("graceful shutdown timed out; exiting");
      process.exit(1);
    }, config.shutdownTimeoutMs).unref();
    stopping = shutdown();
  };
  process.on("SIGINT", requestShutdown);
  process.on("SIGTERM", requestShutdown);

  starting = start();
  try {
    await starting;
  } catch (error) {
    logger.fatal({ err: error }, "edge failed to start");
    process.exitCode = 1;
    await (stopping ??= shutdown());
  }
}
