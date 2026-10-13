import {
  EVENT_ID_HEADER,
  EVENT_TYPE_HEADER,
  type AuctionClosedV1,
} from "@auction-bonanza/contracts";

import type { ConsumedRecord, RecordHeaders } from "./consumed-record.types.js";
import { buildDeadLetterRecord } from "./dead-letter.record.js";
import { decideSettlement } from "./settlement.decision.js";
import { PermanentSettlementError } from "./settlement.errors.js";
import { SETTLEMENT_LOG_MESSAGES } from "./settlement.log-messages.js";
import type {
  DeadLetterReason,
  HandlingOutcome,
  RecordHandler,
  SettlementHandlerOptions,
} from "./settlement.handler.types.js";
import type { SettlementOutcome } from "./settlement.repository.types.js";
import { describeError } from "./shared/errors.js";

// Deals with one auction.events record. It never touches Kafka: the consumer
// hands it a record and acknowledges the record only when handle() returns.
//
// handle() returns when the record is fully dealt with:
//   - ignored: another event type, nothing to do;
//   - settled: the event's effect and its "handled" record are committed;
//   - dead-lettered: the broker acknowledged a copy on the dead-letter topic.
// It throws, and the record is retried, when the database or the dead-letter
// topic fails for a reason that may clear: the record is then neither lost nor
// acknowledged.
export class SettlementHandler implements RecordHandler {
  private readonly options: SettlementHandlerOptions;
  private readonly now: () => Date;

  constructor(options: SettlementHandlerOptions) {
    this.options = options;
    this.now = options.now ?? (() => new Date());
  }

  async handle(record: ConsumedRecord): Promise<HandlingOutcome> {
    const decision = decideSettlement({
      key: record.key?.toString("utf8") ?? null,
      value: record.value?.toString("utf8") ?? null,
      eventType: headerValue(record.headers, EVENT_TYPE_HEADER),
      eventId: headerValue(record.headers, EVENT_ID_HEADER),
    });

    if (decision.kind === "ignore") {
      const fields = {
        partition: record.partition,
        offset: record.offset,
        eventType: decision.eventType,
      };
      if (decision.expected) {
        this.options.logger.debug(fields, SETTLEMENT_LOG_MESSAGES.ignored);
      } else {
        this.options.logger.warn(fields, SETTLEMENT_LOG_MESSAGES.unknownEventType);
      }
      return { kind: "ignored", eventType: decision.eventType };
    }
    if (decision.kind === "reject") {
      await this.deadLetter(record, decision.reason, decision.detail);
      return { kind: "dead_lettered", reason: decision.reason };
    }
    return this.settle(record, decision.event);
  }

  private async settle(record: ConsumedRecord, event: AuctionClosedV1): Promise<HandlingOutcome> {
    let outcome: SettlementOutcome;
    try {
      outcome = await this.options.repository.settle(event);
    } catch (error) {
      if (error instanceof PermanentSettlementError) {
        await this.deadLetter(record, "database-rejected", describePermanentFailure(error));
        return { kind: "dead_lettered", reason: "database-rejected" };
      }
      throw error;
    }
    this.logSettled(event, outcome);
    return { kind: "settled", outcome };
  }

  // Resolves only once the dead-letter topic acknowledged the record. If it
  // does not, the error propagates, the record is retried, and settlement
  // tries the dead-letter send again with it.
  private async deadLetter(
    record: ConsumedRecord,
    reason: DeadLetterReason,
    detail: string,
  ): Promise<void> {
    await this.options.deadLetters.publish(
      buildDeadLetterRecord({
        record,
        cause: { reason, detail },
        consumerGroup: this.options.consumerGroup,
        failedAt: this.now(),
      }),
    );
    this.options.logger.error(
      {
        topic: record.topic,
        partition: record.partition,
        offset: record.offset,
        key: record.key?.toString("utf8") ?? null,
        reason,
        detail,
      },
      SETTLEMENT_LOG_MESSAGES.deadLettered,
    );
  }

  private logSettled(event: AuctionClosedV1, outcome: SettlementOutcome): void {
    const fields = { eventId: event.eventId, auctionId: event.auctionId };
    switch (outcome.kind) {
      case "order_created":
        this.options.logger.info(
          { ...fields, orderId: outcome.orderId },
          SETTLEMENT_LOG_MESSAGES.orderCreated,
        );
        break;
      case "unsold_recorded":
        this.options.logger.info(fields, SETTLEMENT_LOG_MESSAGES.unsoldRecorded);
        break;
      case "duplicate":
        // Replaying retained history (a new or expired group) repeats every
        // close already settled, so one line each would bury the real ones.
        this.options.logger.debug(fields, SETTLEMENT_LOG_MESSAGES.duplicate);
        break;
    }
  }
}

// The database's message plus the rule that fired, so a person reading the
// dead-letter topic can tell a duplicate order from a mismatched winner
// without opening the logs.
function describePermanentFailure(error: PermanentSettlementError): string {
  const rule: string[] = [];
  if (error.sqlState !== undefined) {
    rule.push(`SQLSTATE ${error.sqlState}`);
  }
  if (error.constraint !== undefined) {
    rule.push(`constraint ${error.constraint}`);
  }
  const suffix = rule.length === 0 ? "" : ` [${rule.join(", ")}]`;
  return `${describeError(error)}${suffix}`;
}

function headerValue(headers: RecordHeaders, name: string): string | undefined {
  const value = headers[name];
  const first = Array.isArray(value) ? value[0] : value;
  return first === undefined ? undefined : first.toString();
}
