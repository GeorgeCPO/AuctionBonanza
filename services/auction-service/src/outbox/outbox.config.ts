import { Type, type Static } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

// The publisher's timing budget. Every constant that relates one timeout to
// another lives here, next to the rules that validate it (see "Timeouts" in
// docs/OUTBOX_PUBLISHING.md).

// Extra time the publisher's own timer grants beyond the Kafka delivery
// timeout, so that librdkafka normally reports its own (definitive) timeout
// first. The timer only exists to bound a send that never settles.
export const PUBLISH_TIMEOUT_GRACE_MS = 2_000;

// Part of the shutdown deadline kept back from the producer flush for closing
// the Kafka client and the PostgreSQL pool.
export const SHUTDOWN_CLOSE_MARGIN_MS = 1_000;

// Session limits for the publisher's database connections. Claims and
// publication-state updates are short, index-driven statements, and SKIP
// LOCKED never waits for row locks, so these only stop pathological cases.
export const STATEMENT_TIMEOUT_MS = 10_000;
export const LOCK_TIMEOUT_MS = 5_000;

// Minimum headroom between a Kafka publication giving up and PostgreSQL
// terminating the transaction that holds the claimed rows. PostgreSQL must
// never time out first, or a healthy publisher loses its claim mid-send.
const IDLE_IN_TRANSACTION_HEADROOM_MS = 5_000;

// Time allowed for an in-flight cycle's bookkeeping statements and COMMIT
// after its slowest send has settled. Usually milliseconds; this is a
// practical allowance, not a hard bound (each statement may take up to
// STATEMENT_TIMEOUT_MS).
const SHUTDOWN_COMMIT_HEADROOM_MS = 5_000;

// The shutdown deadline must cover an in-flight cycle's slowest send (the
// publisher's own timer) plus its commit. The producer flush afterwards needs
// no reserved time: every outcome is committed by then, and anything still
// queued is a send already recorded as failed.
const SHUTDOWN_HEADROOM_MS = PUBLISH_TIMEOUT_GRACE_MS + SHUTDOWN_COMMIT_HEADROOM_MS;

const MIN_PUBLISH_TIMEOUT_MS = 1_000;

const brokerPattern = "^[A-Za-z0-9.-]+:[0-9]{1,5}$";

const outboxPublisherConfigSchema = Type.Object(
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
    databaseUrl: Type.String({ pattern: "^postgres(?:ql)?://.+" }),
    kafkaBrokers: Type.Array(Type.String({ pattern: brokerPattern }), {
      minItems: 1,
      maxItems: 32,
    }),
    kafkaClientId: Type.String({ pattern: "^[A-Za-z0-9._-]{1,100}$" }),
    auctionEventsTopic: Type.String({ pattern: "^[A-Za-z0-9._-]{1,249}$" }),
    concurrency: Type.Integer({ minimum: 1, maximum: 16 }),
    batchSize: Type.Integer({ minimum: 1, maximum: 500 }),
    pollIntervalMs: Type.Integer({ minimum: 10, maximum: 60_000 }),
    publishTimeoutMs: Type.Integer({ minimum: MIN_PUBLISH_TIMEOUT_MS, maximum: 120_000 }),
    retryBaseDelayMs: Type.Integer({ minimum: 10, maximum: 60_000 }),
    retryMaxDelayMs: Type.Integer({ minimum: 10, maximum: 3_600_000 }),
    idleInTransactionTimeoutMs: Type.Integer({
      minimum: MIN_PUBLISH_TIMEOUT_MS + IDLE_IN_TRANSACTION_HEADROOM_MS,
      maximum: 600_000,
    }),
    shutdownTimeoutMs: Type.Integer({
      minimum: MIN_PUBLISH_TIMEOUT_MS + SHUTDOWN_HEADROOM_MS,
      maximum: 600_000,
    }),
  },
  { additionalProperties: false },
);

export type OutboxPublisherConfig = Readonly<Static<typeof outboxPublisherConfigSchema>>;

export function parseOutboxPublisherConfig(
  environment: Readonly<Record<string, string | undefined>>,
): OutboxPublisherConfig {
  const candidate = {
    nodeEnv: environment.NODE_ENV ?? "development",
    logLevel: environment.LOG_LEVEL ?? "info",
    databaseUrl: environment.DATABASE_URL,
    kafkaBrokers: (environment.KAFKA_BROKERS ?? "localhost:9092")
      .split(",")
      .map((broker) => broker.trim()),
    kafkaClientId: environment.KAFKA_CLIENT_ID ?? "auction-outbox-publisher",
    auctionEventsTopic: environment.AUCTION_EVENTS_TOPIC ?? "auction.events",
    concurrency: Number(environment.OUTBOX_PUBLISHER_CONCURRENCY ?? "1"),
    batchSize: Number(environment.OUTBOX_BATCH_SIZE ?? "50"),
    pollIntervalMs: Number(environment.OUTBOX_POLL_INTERVAL_MS ?? "250"),
    publishTimeoutMs: Number(environment.OUTBOX_PUBLISH_TIMEOUT_MS ?? "10000"),
    retryBaseDelayMs: Number(environment.OUTBOX_RETRY_BASE_DELAY_MS ?? "500"),
    retryMaxDelayMs: Number(environment.OUTBOX_RETRY_MAX_DELAY_MS ?? "60000"),
    idleInTransactionTimeoutMs: Number(
      environment.OUTBOX_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS ?? "30000",
    ),
    shutdownTimeoutMs: Number(environment.OUTBOX_SHUTDOWN_TIMEOUT_MS ?? "30000"),
  };

  const problems = [...Value.Errors(outboxPublisherConfigSchema, candidate)].map(
    (error) => `${error.path || "/"}: ${error.message}`,
  );
  if (problems.length === 0) {
    problems.push(...relationalProblems(candidate as OutboxPublisherConfig));
  }
  if (problems.length > 0) {
    throw new Error(`Invalid outbox publisher configuration: ${problems.join("; ")}`);
  }

  return candidate as OutboxPublisherConfig;
}

export function loadOutboxPublisherConfig(): OutboxPublisherConfig {
  return parseOutboxPublisherConfig(process.env);
}

function relationalProblems(config: OutboxPublisherConfig): string[] {
  const problems: string[] = [];
  if (config.retryBaseDelayMs > config.retryMaxDelayMs) {
    problems.push("/retryBaseDelayMs: must not exceed retryMaxDelayMs");
  }
  if (
    config.idleInTransactionTimeoutMs <
    config.publishTimeoutMs + IDLE_IN_TRANSACTION_HEADROOM_MS
  ) {
    problems.push(
      `/idleInTransactionTimeoutMs: must be at least publishTimeoutMs + ${IDLE_IN_TRANSACTION_HEADROOM_MS}`,
    );
  }
  if (config.shutdownTimeoutMs < config.publishTimeoutMs + SHUTDOWN_HEADROOM_MS) {
    problems.push(
      `/shutdownTimeoutMs: must be at least publishTimeoutMs + ${SHUTDOWN_HEADROOM_MS} to let an in-flight publication cycle commit`,
    );
  }
  if (new Set(config.kafkaBrokers).size !== config.kafkaBrokers.length) {
    problems.push("/kafkaBrokers: must not contain duplicates");
  }
  return problems;
}
