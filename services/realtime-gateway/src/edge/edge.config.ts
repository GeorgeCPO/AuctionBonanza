import { Type, type Static } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

import {
  hasDuplicates,
  hostPortList,
  logLevelSchema,
  nodeEnvSchema,
  splitList,
} from "../shared/config.js";

const edgeConfigSchema = Type.Object(
  {
    nodeEnv: nodeEnvSchema,
    logLevel: logLevelSchema,
    host: Type.String({ minLength: 1 }),
    // 0 picks a free port; tests use it.
    port: Type.Integer({ minimum: 0, maximum: 65_535 }),
    valkeyNodes: hostPortList(),
    valkeyConnectTimeoutMs: Type.Integer({ minimum: 100, maximum: 60_000 }),
    auctionServiceUrl: Type.String({ pattern: "^https?://[^\\s]+$" }),
    maxAuctionsPerStream: Type.Integer({ minimum: 1, maximum: 100 }),
    // Streams and watched auctions at once on this edge. The endpoint is
    // public, so both are bounded; new requests beyond them get 503.
    maxConnections: Type.Integer({ minimum: 1, maximum: 1_000_000 }),
    maxWatchedAuctions: Type.Integer({ minimum: 1, maximum: 10_000_000 }),
    // Well below the idle timeout of every proxy in front of the edge (60 s on
    // an AWS ALB by default), so that one late heartbeat does not close a
    // quiet stream.
    heartbeatIntervalMs: Type.Integer({ minimum: 100, maximum: 30_000 }),
    // How long an auction stays subscribed after its last client left.
    lingerMs: Type.Integer({ minimum: 0, maximum: 600_000 }),
    liveStateBatchDelayMs: Type.Integer({ minimum: 0, maximum: 1_000 }),
    liveStateTimeoutMs: Type.Integer({ minimum: 100, maximum: 60_000 }),
    liveStateConcurrency: Type.Integer({ minimum: 1, maximum: 64 }),
    retryBaseDelayMs: Type.Integer({ minimum: 10, maximum: 60_000 }),
    retryMaxDelayMs: Type.Integer({ minimum: 10, maximum: 3_600_000 }),
    maxBufferedEventsPerAuction: Type.Integer({ minimum: 1, maximum: 1_000_000 }),
    maxBufferedBytesPerConnection: Type.Integer({ minimum: 1_024, maximum: 64 * 1_024 * 1_024 }),
    shutdownTimeoutMs: Type.Integer({ minimum: 1_000, maximum: 600_000 }),
  },
  { additionalProperties: false },
);

export type EdgeConfig = Readonly<Static<typeof edgeConfigSchema>>;

export function parseEdgeConfig(
  environment: Readonly<Record<string, string | undefined>>,
): EdgeConfig {
  const candidate = {
    nodeEnv: environment.NODE_ENV ?? "development",
    logLevel: environment.LOG_LEVEL ?? "info",
    host: environment.HOST ?? "0.0.0.0",
    port: Number(environment.PORT ?? "3003"),
    valkeyNodes: splitList(environment.VALKEY_NODES ?? ""),
    valkeyConnectTimeoutMs: Number(environment.VALKEY_CONNECT_TIMEOUT_MS ?? "2000"),
    auctionServiceUrl: environment.AUCTION_SERVICE_URL ?? "http://localhost:3002",
    maxAuctionsPerStream: Number(environment.EDGE_MAX_AUCTIONS_PER_STREAM ?? "50"),
    maxConnections: Number(environment.EDGE_MAX_CONNECTIONS ?? "10000"),
    maxWatchedAuctions: Number(environment.EDGE_MAX_WATCHED_AUCTIONS ?? "100000"),
    heartbeatIntervalMs: Number(environment.EDGE_HEARTBEAT_INTERVAL_MS ?? "15000"),
    lingerMs: Number(environment.EDGE_LINGER_MS ?? "30000"),
    liveStateBatchDelayMs: Number(environment.EDGE_LIVE_STATE_BATCH_DELAY_MS ?? "5"),
    liveStateTimeoutMs: Number(environment.EDGE_LIVE_STATE_TIMEOUT_MS ?? "2000"),
    liveStateConcurrency: Number(environment.EDGE_LIVE_STATE_CONCURRENCY ?? "4"),
    retryBaseDelayMs: Number(environment.EDGE_RETRY_BASE_DELAY_MS ?? "200"),
    retryMaxDelayMs: Number(environment.EDGE_RETRY_MAX_DELAY_MS ?? "10000"),
    maxBufferedEventsPerAuction: Number(environment.EDGE_MAX_BUFFERED_EVENTS ?? "1000"),
    maxBufferedBytesPerConnection: Number(
      environment.EDGE_MAX_BUFFERED_BYTES_PER_CONNECTION ?? "1048576",
    ),
    shutdownTimeoutMs: Number(environment.EDGE_SHUTDOWN_TIMEOUT_MS ?? "10000"),
  };

  const problems = [...Value.Errors(edgeConfigSchema, candidate)].map(
    (error) => `${error.path || "/"}: ${error.message}`,
  );
  if (problems.length === 0) {
    problems.push(...relationalProblems(candidate as EdgeConfig));
  }
  if (problems.length > 0) {
    throw new Error(`Invalid edge configuration: ${problems.join("; ")}`);
  }

  return candidate as EdgeConfig;
}

export function loadEdgeConfig(): EdgeConfig {
  return parseEdgeConfig(process.env);
}

function relationalProblems(config: EdgeConfig): string[] {
  const problems: string[] = [];
  if (config.retryBaseDelayMs > config.retryMaxDelayMs) {
    problems.push("/retryBaseDelayMs: must not exceed retryMaxDelayMs");
  }
  if (hasDuplicates(config.valkeyNodes)) {
    problems.push("/valkeyNodes: must not contain duplicates");
  }
  return problems;
}
