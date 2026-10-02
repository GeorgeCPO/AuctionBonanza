import { Type, type Static } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

// Time allowed on shutdown, beyond the slowest in-flight publish, for the
// consumer to commit its stored offsets and leave the group.
const SHUTDOWN_HEADROOM_MS = 1_000;

const kafkaNamePattern = "^[A-Za-z0-9._-]{1,100}$";

function hostPortList() {
  return Type.Array(Type.String({ pattern: "^[A-Za-z0-9.-]+:[0-9]{1,5}$" }), {
    minItems: 1,
    maxItems: 32,
  });
}

const relayConfigSchema = Type.Object(
  {
    nodeEnv: Type.Union([
      Type.Literal("development"),
      Type.Literal("test"),
      Type.Literal("production"),
    ]),
    logLevel: Type.Union([
      Type.Literal("fatal"),
      Type.Literal("error"),
      Type.Literal("warn"),
      Type.Literal("info"),
      Type.Literal("debug"),
      Type.Literal("trace"),
    ]),
    kafkaBrokers: hostPortList(),
    kafkaClientId: Type.String({ pattern: kafkaNamePattern }),
    kafkaGroupId: Type.String({ pattern: kafkaNamePattern }),
    auctionEventsTopic: Type.String({ pattern: "^[A-Za-z0-9._-]{1,249}$" }),
    valkeyNodes: hostPortList(),
    // How many partitions one relay process works on at the same time. Each
    // partition is still processed strictly in offset order.
    partitionConcurrency: Type.Integer({ minimum: 1, maximum: 64 }),
    // Bounds one Valkey publish, including time queued while a node
    // reconnects, so a failed publish is retried instead of hanging.
    publishTimeoutMs: Type.Integer({ minimum: 100, maximum: 60_000 }),
    // How long the group waits for a relay that stopped heartbeating (for
    // example, a crashed process) before giving its partitions to another
    // relay. It bounds how long those partitions' live updates stall. The
    // broker's group.min.session.timeout.ms (6 s by default) is the floor.
    sessionTimeoutMs: Type.Integer({ minimum: 6_000, maximum: 300_000 }),
    commitIntervalMs: Type.Integer({ minimum: 100, maximum: 60_000 }),
    retryBaseDelayMs: Type.Integer({ minimum: 10, maximum: 60_000 }),
    retryMaxDelayMs: Type.Integer({ minimum: 10, maximum: 3_600_000 }),
    shutdownTimeoutMs: Type.Integer({ minimum: 1_000, maximum: 600_000 }),
  },
  { additionalProperties: false },
);

export type RelayConfig = Readonly<Static<typeof relayConfigSchema>>;

export function parseRelayConfig(
  environment: Readonly<Record<string, string | undefined>>,
): RelayConfig {
  const candidate = {
    nodeEnv: environment.NODE_ENV ?? "development",
    logLevel: environment.LOG_LEVEL ?? "info",
    kafkaBrokers: splitList(environment.KAFKA_BROKERS ?? "localhost:9092"),
    kafkaClientId: environment.KAFKA_CLIENT_ID ?? "realtime-relay",
    kafkaGroupId: environment.KAFKA_GROUP_ID ?? "realtime-relay",
    auctionEventsTopic: environment.AUCTION_EVENTS_TOPIC ?? "auction.events",
    valkeyNodes: splitList(environment.VALKEY_NODES ?? ""),
    partitionConcurrency: Number(environment.RELAY_PARTITION_CONCURRENCY ?? "4"),
    publishTimeoutMs: Number(environment.RELAY_PUBLISH_TIMEOUT_MS ?? "2000"),
    sessionTimeoutMs: Number(environment.RELAY_SESSION_TIMEOUT_MS ?? "10000"),
    commitIntervalMs: Number(environment.RELAY_COMMIT_INTERVAL_MS ?? "1000"),
    retryBaseDelayMs: Number(environment.RELAY_RETRY_BASE_DELAY_MS ?? "200"),
    retryMaxDelayMs: Number(environment.RELAY_RETRY_MAX_DELAY_MS ?? "10000"),
    shutdownTimeoutMs: Number(environment.RELAY_SHUTDOWN_TIMEOUT_MS ?? "15000"),
  };

  const problems = [...Value.Errors(relayConfigSchema, candidate)].map(
    (error) => `${error.path || "/"}: ${error.message}`,
  );
  if (problems.length === 0) {
    problems.push(...relationalProblems(candidate as RelayConfig));
  }
  if (problems.length > 0) {
    throw new Error(`Invalid relay configuration: ${problems.join("; ")}`);
  }

  return candidate as RelayConfig;
}

export function loadRelayConfig(): RelayConfig {
  return parseRelayConfig(process.env);
}

function splitList(value: string): string[] {
  return value === "" ? [] : value.split(",").map((item) => item.trim());
}

function relationalProblems(config: RelayConfig): string[] {
  const problems: string[] = [];
  if (config.retryBaseDelayMs > config.retryMaxDelayMs) {
    problems.push("/retryBaseDelayMs: must not exceed retryMaxDelayMs");
  }
  // Shutdown waits for in-flight publishes, each bounded by the publish
  // timeout, before the consumer commits and leaves the group.
  if (config.shutdownTimeoutMs < config.publishTimeoutMs + SHUTDOWN_HEADROOM_MS) {
    problems.push(
      `/shutdownTimeoutMs: must be at least publishTimeoutMs + ${SHUTDOWN_HEADROOM_MS}`,
    );
  }
  if (new Set(config.kafkaBrokers).size !== config.kafkaBrokers.length) {
    problems.push("/kafkaBrokers: must not contain duplicates");
  }
  if (new Set(config.valkeyNodes).size !== config.valkeyNodes.length) {
    problems.push("/valkeyNodes: must not contain duplicates");
  }
  return problems;
}
