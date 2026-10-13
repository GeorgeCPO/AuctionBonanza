import type { Logger } from "pino";

import type { Database } from "../../src/shared/database.types.js";
import type { FlakyDeadLetters, FlakyRepository } from "./settlement-harness.js";

export interface StartSettlementOptions {
  brokers: string;
  topic: string;
  deadLetterTopic: string;
  groupId: string;
  database: Database;
  logger?: Logger;
}

export interface SettlementRun {
  repository: FlakyRepository;
  deadLetters: FlakyDeadLetters;
  stop(): Promise<void>;
}
