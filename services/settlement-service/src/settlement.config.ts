import { Type, type Static } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

import {
  hasDuplicates,
  hostPortList,
  logLevelSchema,
  nodeEnvSchema,
  splitList,
} from "./shared/config.js";

// Session limits for the settlement worker's database connections. A
// settlement is a few short statements, so these only stop pathological cases.
// The idle-in-transaction limit ends the session of a worker that stalled
// mid-transaction, which rolls the transaction back and frees its locks.
export const SETTLEMENT_STATEMENT_TIMEOUT_MS = 10_000;
export const SETTLEMENT_LOCK_TIMEOUT_MS = 5_000;
export const SETTLEMENT_IDLE_IN_TRANSACTION_TIMEOUT_MS = 30_000;

// The dead-letter producer derives librdkafka's socket.connection.setup.timeout.ms
// from its send timeout, and librdkafka rejects that property below 1000 ms. A
// lower send timeout would pass validation here and then fail the producer's
// construction at startup, naming no setting of ours.
const MIN_DEAD_LETTER_TIMEOUT_MS = 1_000;

// Time allowed on shutdown, beyond the slowest in-flight dead-letter send, for
// the consumer to commit its stored offsets and leave the group.
const SHUTDOWN_HEADROOM_MS = 1_000;

const kafkaNamePattern = "^[A-Za-z0-9._-]{1,100}$";
const topicPattern = "^[A-Za-z0-9._-]{1,249}$";

const settlementConfigSchema = Type.Object(
  {
    nodeEnv: nodeEnvSchema,
    logLevel: logLevelSchema,
    databaseUrl: Type.String({ pattern: "^postgres(?:ql)?://.+" }),
    kafkaBrokers: hostPortList(),
    kafkaClientId: Type.String({ pattern: kafkaNamePattern }),
    kafkaGroupId: Type.String({ pattern: kafkaNamePattern }),
    auctionEventsTopic: Type.String({ pattern: topicPattern }),
    deadLetterTopic: Type.String({ pattern: topicPattern }),
    // How many partitions one worker process works on at the same time. Each
    // partition is still handled strictly in offset order, and each message in
    // flight holds one database connection, so this is also the pool size.
    partitionConcurrency: Type.Integer({ minimum: 1, maximum: 64 }),
    // Bounds one send to the dead-letter topic, so a failed send is retried
    // instead of hanging the partition.
    deadLetterTimeoutMs: Type.Integer({ minimum: MIN_DEAD_LETTER_TIMEOUT_MS, maximum: 60_000 }),
    // How long the group waits for a worker that stopped heartbeating (for
    // example, a crashed process) before giving its partitions to another
    // worker. It bounds how long those partitions' settlements stall. The
    // broker's group.min.session.timeout.ms (6 s by default) is the floor.
    sessionTimeoutMs: Type.Integer({ minimum: 6_000, maximum: 300_000 }),
    commitIntervalMs: Type.Integer({ minimum: 100, maximum: 60_000 }),
    retryBaseDelayMs: Type.Integer({ minimum: 10, maximum: 60_000 }),
    retryMaxDelayMs: Type.Integer({ minimum: 10, maximum: 3_600_000 }),
    shutdownTimeoutMs: Type.Integer({ minimum: 1_000, maximum: 600_000 }),
  },
  { additionalProperties: false },
);

export type SettlementConfig = Readonly<Static<typeof settlementConfigSchema>>;

export function parseSettlementConfig(
  environment: Readonly<Record<string, string | undefined>>,
): SettlementConfig {
  const candidate = {
    nodeEnv: environment.NODE_ENV ?? "development",
    logLevel: environment.LOG_LEVEL ?? "info",
    databaseUrl: environment.DATABASE_URL,
    kafkaBrokers: splitList(environment.KAFKA_BROKERS ?? "localhost:9092"),
    kafkaClientId: environment.KAFKA_CLIENT_ID ?? "settlement-worker",
    kafkaGroupId: environment.KAFKA_GROUP_ID ?? "settlement-worker",
    auctionEventsTopic: environment.AUCTION_EVENTS_TOPIC ?? "auction.events",
    deadLetterTopic: environment.SETTLEMENT_DEAD_LETTER_TOPIC ?? "auction.events.dlq",
    partitionConcurrency: Number(environment.SETTLEMENT_PARTITION_CONCURRENCY ?? "4"),
    deadLetterTimeoutMs: Number(environment.SETTLEMENT_DEAD_LETTER_TIMEOUT_MS ?? "5000"),
    sessionTimeoutMs: Number(environment.SETTLEMENT_SESSION_TIMEOUT_MS ?? "10000"),
    commitIntervalMs: Number(environment.SETTLEMENT_COMMIT_INTERVAL_MS ?? "1000"),
    retryBaseDelayMs: Number(environment.SETTLEMENT_RETRY_BASE_DELAY_MS ?? "200"),
    retryMaxDelayMs: Number(environment.SETTLEMENT_RETRY_MAX_DELAY_MS ?? "10000"),
    shutdownTimeoutMs: Number(environment.SETTLEMENT_SHUTDOWN_TIMEOUT_MS ?? "15000"),
  };

  const problems = [...Value.Errors(settlementConfigSchema, candidate)].map(
    (error) => `${error.path || "/"}: ${error.message}`,
  );
  if (problems.length === 0) {
    problems.push(...relationalProblems(candidate as SettlementConfig));
  }
  if (problems.length > 0) {
    throw new Error(`Invalid settlement configuration: ${problems.join("; ")}`);
  }

  return candidate as SettlementConfig;
}

export function loadSettlementConfig(): SettlementConfig {
  return parseSettlementConfig(process.env);
}

function relationalProblems(config: SettlementConfig): string[] {
  const problems: string[] = [];
  if (config.retryBaseDelayMs > config.retryMaxDelayMs) {
    problems.push("/retryBaseDelayMs: must not exceed retryMaxDelayMs");
  }
  // Shutdown waits for in-flight messages, each bounded by the dead-letter
  // timeout when it is sending, before the consumer commits and leaves the
  // group.
  if (config.shutdownTimeoutMs < config.deadLetterTimeoutMs + SHUTDOWN_HEADROOM_MS) {
    problems.push(
      `/shutdownTimeoutMs: must be at least deadLetterTimeoutMs + ${SHUTDOWN_HEADROOM_MS}`,
    );
  }
  // A dead-letter topic that is also the input topic would feed every
  // rejected record back to the worker that rejected it, forever.
  if (config.deadLetterTopic === config.auctionEventsTopic) {
    problems.push("/deadLetterTopic: must differ from auctionEventsTopic");
  }
  if (hasDuplicates(config.kafkaBrokers)) {
    problems.push("/kafkaBrokers: must not contain duplicates");
  }
  return problems;
}
