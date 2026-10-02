import { KafkaJS } from "@confluentinc/kafka-javascript";
import type { Logger } from "pino";

import type { BusPublisher } from "../bus/bus.js";
import { backoffDelayMs, type RetryBackoffPolicy } from "../shared/backoff.js";
import { decideRelay, type SkipDecision } from "./relay.decision.js";

export interface EventRelayOptions {
  brokers: readonly string[];
  clientId: string;
  groupId: string;
  topic: string;
  publisher: BusPublisher;
  partitionConcurrency: number;
  commitIntervalMs: number;
  sessionTimeoutMs: number;
  retryBackoff: RetryBackoffPolicy;
  logger: Logger;
}

interface SkippedRecord {
  partition: number;
  offset: string;
  key: string | null;
  eventId: string | undefined;
}

// Forwards auction.events to the Valkey bus. See "Relay" in
// docs/REALTIME_GATEWAY.md.
//
// Acknowledgement boundary: the Kafka client stores a message's offset only
// after its handler resolves, and auto-commit only ever commits stored
// offsets. The handler resolves after the bus accepted the publish (or after
// deciding to skip the record), so an offset is never committed ahead of its
// publish. A crash in between re-publishes; edges drop the duplicate.
//
// Ordering: within a partition, messages are handled one at a time in offset
// order, and a failed message is retried before any later one.
export class EventRelay {
  private readonly options: EventRelayOptions;
  private readonly logger: Logger;
  private readonly consumer: KafkaJS.Consumer;
  // Consecutive publish failures per partition, for the retry backoff.
  private readonly failures = new Map<number, number>();
  private readonly resumeTimers = new Set<NodeJS.Timeout>();
  private stopping = false;

  constructor(options: EventRelayOptions) {
    this.options = options;
    this.logger = options.logger;
    this.consumer = new KafkaJS.Kafka({}).consumer({
      kafkaJS: {
        groupId: options.groupId,
        // A partition without a committed offset (a new group, or offsets
        // the broker expired) starts at the oldest retained event, so no
        // event is skipped. Edges drop the ones older than their state.
        fromBeginning: true,
        autoCommit: true,
        autoCommitInterval: options.commitIntervalMs,
        sessionTimeout: options.sessionTimeoutMs,
        // A rebalance moves only the partitions that must move; the other
        // partitions keep flowing on their current relay.
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
      eachMessage: async (payload) => this.relayMessage(payload),
    });
  }

  // Waits for in-flight messages, then leaves the group. Offsets stored by
  // then are committed on the way out.
  async stop(): Promise<void> {
    this.stopping = true;
    for (const timer of this.resumeTimers) clearTimeout(timer);
    this.resumeTimers.clear();
    await this.consumer.disconnect();
  }

  private async relayMessage({
    topic,
    partition,
    message,
  }: KafkaJS.EachMessagePayload): Promise<void> {
    try {
      const key = message.key?.toString("utf8") ?? null;
      const decision = decideRelay({
        key,
        value: message.value?.toString("utf8") ?? null,
        eventType: headerValue(message.headers, "event-type"),
      });
      if (decision.kind === "publish") {
        await this.options.publisher.publish(decision.channel, decision.message);
      } else {
        this.logSkip(decision, {
          partition,
          offset: message.offset,
          key,
          eventId: headerValue(message.headers, "event-id"),
        });
      }
      this.failures.delete(partition);
    } catch (error) {
      this.pauseAfterFailure(topic, partition, message.offset, error);
      // The client seeks back to this message, so it is delivered again
      // once the partition resumes.
      throw error;
    }
  }

  // An unknown event type is most likely one this relay has not been updated
  // for, so it is a warning; a malformed record is a producer bug. Both
  // messages start with "skipping", so every skipped record can be found in
  // the logs.
  private logSkip(skip: SkipDecision, record: SkippedRecord): void {
    const details = { ...record, reason: skip.reason, detail: skip.detail };
    if (skip.reason === "unknown-event-type") {
      this.logger.warn(details, "skipping auction event with no relay contract");
    } else {
      this.logger.error(details, "skipping malformed auction event; edges will resync on the gap");
    }
  }

  // Pausing, rather than waiting inside the handler, keeps the consumer
  // polling, so a long outage does not exceed max.poll.interval.ms and evict
  // this relay from the group. Only the failing partition waits.
  private pauseAfterFailure(
    topic: string,
    partition: number,
    offset: string,
    error: unknown,
  ): void {
    const attempt = (this.failures.get(partition) ?? 0) + 1;
    this.failures.set(partition, attempt);
    const retryDelayMs = backoffDelayMs(this.options.retryBackoff, attempt);
    this.logger.error(
      { err: error, partition, offset, attempt, retryDelayMs },
      "relay publish failed; partition paused, offset not committed",
    );

    const partitions = [{ topic, partitions: [partition] }];
    this.consumer.pause(partitions);
    const timer = setTimeout(() => {
      this.resumeTimers.delete(timer);
      if (this.stopping) return;
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

function headerValue(headers: KafkaJS.IHeaders | undefined, name: string): string | undefined {
  const value = headers?.[name];
  const first = Array.isArray(value) ? value[0] : value;
  return first === undefined ? undefined : first.toString();
}

// The same adapter as auction-service's outbox transport, copied rather than
// shared. The client's info and debug output is chatty, so both move down
// one level.
function createKafkaLogger(logger: Logger): KafkaJS.Logger {
  const child = logger.child({ component: "kafka" });
  const kafkaLogger: KafkaJS.Logger = {
    info: (message, extra) => child.debug(extra ?? {}, message),
    debug: (message, extra) => child.trace(extra ?? {}, message),
    warn: (message, extra) => child.warn(extra ?? {}, message),
    error: (message, extra) => child.error(extra ?? {}, message),
    namespace: () => kafkaLogger,
    setLogLevel: () => {},
  };
  return kafkaLogger;
}
