import { KafkaJS } from "@confluentinc/kafka-javascript";
import type { Logger } from "pino";

import type { ConsumedRecord } from "./consumed-record.types.js";
import type { SettlementConsumerOptions } from "./settlement.consumer.types.js";
import { SETTLEMENT_LOG_MESSAGES } from "./settlement.log-messages.js";
import { backoffDelayMs } from "./shared/backoff.js";
import { createKafkaLogger } from "./shared/kafka-logger.js";

// Reads auction.events and hands each record to the handler. See "Settlement"
// in docs/SETTLEMENT.md.
//
// Acknowledgement boundary: the Kafka client stores a message's offset only
// after its handler resolves, and auto-commit only ever commits stored
// offsets. The handler resolves after the record's database transaction
// committed (or after the dead-letter topic acknowledged a copy), so an offset
// is never committed ahead of its effect. A crash between the commit and the
// offset commit delivers the record again, and the handler recognises it as a
// duplicate.
//
// Ordering: within a partition, records are handled one at a time in offset
// order, and a failed record is retried before any later one. All events of an
// auction share a key, so they share a partition.
//
// A record is never skipped on failure. The handler either deals with it
// completely or throws, and a thrown record pauses its partition and is
// delivered again.
export class SettlementConsumer {
  private readonly options: SettlementConsumerOptions;
  private readonly logger: Logger;
  private readonly consumer: KafkaJS.Consumer;
  // Consecutive failures per partition, for the retry backoff.
  private readonly failures = new Map<number, number>();
  private readonly resumeTimers = new Set<NodeJS.Timeout>();
  private stopping = false;

  constructor(options: SettlementConsumerOptions) {
    this.options = options;
    this.logger = options.logger;
    this.consumer = new KafkaJS.Kafka({}).consumer({
      kafkaJS: {
        groupId: options.groupId,
        // A partition without a committed offset (a new group, or offsets the
        // broker expired) starts at the oldest retained event, so no closed
        // auction is skipped. Settling an event again is harmless.
        fromBeginning: true,
        autoCommit: true,
        autoCommitInterval: options.commitIntervalMs,
        sessionTimeout: options.sessionTimeoutMs,
        // A rebalance moves only the partitions that must move; the other
        // partitions keep flowing on their current consumer.
        partitionAssigners: [KafkaJS.PartitionAssigners.cooperativeSticky],
        logger: createKafkaLogger(options.logger),
      },
      "bootstrap.servers": options.brokers.join(","),
      "client.id": options.clientId,
    });
  }

  async start(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [this.options.topic] });
    await this.consumer.run({
      partitionsConsumedConcurrently: this.options.partitionConcurrency,
      eachMessage: async (payload) => this.consumeMessage(payload),
    });
  }

  // Waits for in-flight messages, then leaves the group. Offsets stored by
  // then are committed on the way out.
  async stop(): Promise<void> {
    this.stopping = true;
    for (const timer of this.resumeTimers) {
      clearTimeout(timer);
    }
    this.resumeTimers.clear();
    await this.consumer.disconnect();
  }

  private async consumeMessage({
    topic,
    partition,
    message,
  }: KafkaJS.EachMessagePayload): Promise<void> {
    const record: ConsumedRecord = {
      topic,
      partition,
      offset: message.offset,
      key: message.key,
      value: message.value,
      headers: message.headers ?? {},
    };
    try {
      await this.options.handler.handle(record);
      this.failures.delete(partition);
    } catch (error) {
      this.pauseAfterFailure(record, error);
      // The client seeks back to this message, so it is delivered again once
      // the partition resumes.
      throw error;
    }
  }

  // Pausing, rather than waiting inside the handler, keeps the consumer
  // polling, so a long outage does not exceed max.poll.interval.ms and evict
  // this consumer from the group. Only the failing partition waits.
  private pauseAfterFailure({ topic, partition, offset }: ConsumedRecord, error: unknown): void {
    const attempt = (this.failures.get(partition) ?? 0) + 1;
    this.failures.set(partition, attempt);
    const retryDelayMs = backoffDelayMs(this.options.retryBackoff, attempt);
    this.logger.error(
      { err: error, partition, offset, attempt, retryDelayMs },
      SETTLEMENT_LOG_MESSAGES.partitionPaused,
    );

    const partitions = [{ topic, partitions: [partition] }];
    this.consumer.pause(partitions);
    const timer = setTimeout(() => {
      this.resumeTimers.delete(timer);
      if (this.stopping) {
        return;
      }
      try {
        this.consumer.resume(partitions);
      } catch (resumeError) {
        // The partition may have been revoked while paused; its new owner
        // starts from the committed offset.
        this.logger.debug({ err: resumeError, partition }, "could not resume partition");
      }
    }, retryDelayMs);
    this.resumeTimers.add(timer);
  }
}
