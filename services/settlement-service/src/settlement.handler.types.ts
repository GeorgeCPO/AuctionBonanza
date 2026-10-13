import type { Logger } from "pino";

import type { ConsumedRecord } from "./consumed-record.types.js";
import type { DeadLetterPublisher } from "./dead-letter.types.js";
import type { RejectionReason } from "./settlement.decision.types.js";
import type { SettlementOutcome, SettlementRepository } from "./settlement.repository.types.js";

// Why a record went to the dead-letter topic: it could not be read as an
// AuctionClosed.v1, or the database refused its data.
export type DeadLetterReason = RejectionReason | "database-rejected";

// What the handler did with a record. The record is fully dealt with in every
// case, so the caller may acknowledge it. A record the handler could not deal
// with is not an outcome: handle() throws, and the caller retries it.
export type HandlingOutcome =
  | { kind: "ignored"; eventType: string }
  | { kind: "settled"; outcome: SettlementOutcome }
  | { kind: "dead_lettered"; reason: DeadLetterReason };

export interface RecordHandler {
  handle(record: ConsumedRecord): Promise<HandlingOutcome>;
}

export interface SettlementHandlerOptions {
  repository: SettlementRepository;
  deadLetters: DeadLetterPublisher;
  consumerGroup: string;
  logger: Logger;
  now?: () => Date;
}
