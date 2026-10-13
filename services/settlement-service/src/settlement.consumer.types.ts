import type { Logger } from "pino";

import type { RecordHandler } from "./settlement.handler.types.js";
import type { RetryBackoffPolicy } from "./shared/backoff.js";

export interface SettlementConsumerOptions {
  brokers: readonly string[];
  clientId: string;
  groupId: string;
  topic: string;
  handler: RecordHandler;
  partitionConcurrency: number;
  commitIntervalMs: number;
  sessionTimeoutMs: number;
  retryBackoff: RetryBackoffPolicy;
  logger: Logger;
}
