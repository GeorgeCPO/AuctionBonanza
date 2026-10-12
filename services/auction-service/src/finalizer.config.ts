import { Type, type Static } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

// Session limits for the finalizer's database connection. A close is a few
// short statements, so these only stop pathological cases. The idle-in-
// transaction limit releases the lock of a finalizer that stalled mid-close.
export const FINALIZER_STATEMENT_TIMEOUT_MS = 10_000;
export const FINALIZER_LOCK_TIMEOUT_MS = 5_000;
export const FINALIZER_IDLE_IN_TRANSACTION_TIMEOUT_MS = 30_000;

const finalizerConfigSchema = Type.Object(
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
    concurrency: Type.Integer({ minimum: 1, maximum: 16 }),
    pollIntervalMs: Type.Integer({ minimum: 10, maximum: 60_000 }),
    retryBaseDelayMs: Type.Integer({ minimum: 10, maximum: 60_000 }),
    retryMaxDelayMs: Type.Integer({ minimum: 10, maximum: 3_600_000 }),
    shutdownTimeoutMs: Type.Integer({ minimum: 1_000, maximum: 600_000 }),
  },
  { additionalProperties: false },
);

export type FinalizerConfig = Readonly<Static<typeof finalizerConfigSchema>>;

export function parseFinalizerConfig(
  environment: Readonly<Record<string, string | undefined>> = process.env,
): FinalizerConfig {
  const candidate = {
    nodeEnv: environment.NODE_ENV ?? "development",
    logLevel: environment.LOG_LEVEL ?? "info",
    databaseUrl: environment.DATABASE_URL,
    concurrency: Number(environment.FINALIZER_CONCURRENCY ?? "1"),
    pollIntervalMs: Number(environment.FINALIZER_POLL_INTERVAL_MS ?? "1000"),
    retryBaseDelayMs: Number(environment.FINALIZER_RETRY_BASE_DELAY_MS ?? "500"),
    retryMaxDelayMs: Number(environment.FINALIZER_RETRY_MAX_DELAY_MS ?? "30000"),
    shutdownTimeoutMs: Number(environment.FINALIZER_SHUTDOWN_TIMEOUT_MS ?? "15000"),
  };

  const problems = [...Value.Errors(finalizerConfigSchema, candidate)].map(
    (error) => `${error.path || "/"}: ${error.message}`,
  );
  if (problems.length === 0 && candidate.retryMaxDelayMs < candidate.retryBaseDelayMs) {
    problems.push("FINALIZER_RETRY_MAX_DELAY_MS must be at least FINALIZER_RETRY_BASE_DELAY_MS");
  }
  if (problems.length > 0) {
    throw new Error(`Invalid finalizer configuration: ${problems.join("; ")}`);
  }

  return candidate as FinalizerConfig;
}
