import { KafkaJS } from "@confluentinc/kafka-javascript";
import type { Logger } from "pino";

import type { BusPublisher } from "../bus/bus.js";
import { backoffDelayMs, type RetryBackoffPolicy } from "../shared/backoff.js";
import { decideRelay, type SkipDecision } from "./relay.decision.js";
import { ResyncBroadcaster } from "./relay.resync.js";

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

interface TopicPartition {
  topic: string;
  partition: number;
}

interface PartitionState {
  // The group's committed offset when this relay was assigned the partition,
  // or null when there was none.
  committedOffset: bigint | null;
  firstMessageChecked: boolean;
  consecutiveFailures: number;
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
  private readonly resync: ResyncBroadcaster;
  private readonly partitions = new Map<number, PartitionState>();
  private readonly resumeTimers = new Set<NodeJS.Timeout>();
  private readonly stopping = new AbortController();

  constructor(options: EventRelayOptions) {
    this.options = options;
    this.logger = options.logger;
    this.resync = new ResyncBroadcaster({
      publisher: options.publisher,
      retryBackoff: options.retryBackoff,
      logger: options.logger,
      signal: this.stopping.signal,
    });
    this.consumer = new KafkaJS.Kafka({}).consumer({
      kafkaJS: {
        groupId: options.groupId,
        // A partition without a committed offset starts at the end: edges
        // start from live-state snapshots, not from history. The relay then
        // broadcasts a resync, because events before that point were skipped.
        fromBeginning: false,
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
      // Awaited by the client before it applies an assignment, so partition
      // state (and any resync request) exists before the first message.
      rebalance_cb: async (error: { code: number }, assignment: TopicPartition[]) => {
        await this.onRebalance(error, assignment);
      },
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
    this.stopping.abort();
    for (const timer of this.resumeTimers) clearTimeout(timer);
    this.resumeTimers.clear();
    await this.consumer.disconnect();
  }

  private async relayMessage({
    topic,
    partition,
    message,
  }: KafkaJS.EachMessagePayload): Promise<void> {
    const state = this.partitionState(partition);
    const offset = BigInt(message.offset);
    try {
      this.checkContinuity(partition, state, offset);
      // Edges must refresh before they see anything that follows a gap.
      await this.resync.publishPending();

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
      state.consecutiveFailures = 0;
    } catch (error) {
      this.pauseAfterFailure(topic, partition, state, message.offset, error);
      // The client seeks back to this message, so it is delivered again
      // once the partition resumes.
      throw error;
    }
  }

  private async onRebalance(error: { code: number }, assignment: TopicPartition[]): Promise<void> {
    if (error.code === KafkaJS.ErrorCodes.ERR__ASSIGN_PARTITIONS) {
      await this.onAssigned(assignment);
    } else {
      this.onRevoked(assignment);
    }
  }

  private async onAssigned(assignment: TopicPartition[]): Promise<void> {
    const committed = await this.readCommittedOffsets(assignment);
    const withoutCommit: number[] = [];
    for (const { partition } of assignment) {
      const committedOffset = committed.get(partition) ?? null;
      if (committedOffset === null) withoutCommit.push(partition);
      this.partitions.set(partition, {
        committedOffset,
        firstMessageChecked: false,
        consecutiveFailures: 0,
      });
    }
    this.logger.info(
      {
        partitions: assignment.map(({ partition }) => ({
          partition,
          committedOffset: committed.get(partition)?.toString() ?? null,
        })),
      },
      "partitions assigned",
    );
    if (withoutCommit.length > 0) {
      this.resync.request(
        `partitions ${withoutCommit.join(",")} started without a committed offset`,
      );
    }
  }

  private onRevoked(assignment: TopicPartition[]): void {
    for (const { partition } of assignment) this.partitions.delete(partition);
    this.logger.info(
      { partitions: assignment.map(({ partition }) => partition) },
      "partitions revoked",
    );
  }

  // A partition missing from the result has no committed offset.
  private async readCommittedOffsets(
    assignment: TopicPartition[],
  ): Promise<Map<number, bigint | null>> {
    const committed = new Map<number, bigint | null>();
    try {
      const offsets = await this.consumer.committed(assignment);
      for (const { partition, offset } of offsets) {
        committed.set(partition, parseCommittedOffset(offset));
      }
    } catch (error) {
      // Without the committed offsets, a skipped range cannot be ruled out.
      this.logger.warn({ err: error }, "could not read committed offsets; treating them as absent");
    }
    return committed;
  }

  // The first message after an assignment must be the committed offset. Any
  // other offset means the client reset its position (for example because
  // retention deleted the committed offset) and events may have been skipped.
  private checkContinuity(partition: number, state: PartitionState, offset: bigint): void {
    if (state.firstMessageChecked) return;
    state.firstMessageChecked = true;
    if (state.committedOffset !== null && offset !== state.committedOffset) {
      this.resync.request(
        `partition ${partition} resumed at offset ${offset}, not at committed offset ${state.committedOffset}`,
      );
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
    state: PartitionState,
    offset: string,
    error: unknown,
  ): void {
    state.consecutiveFailures += 1;
    const retryDelayMs = backoffDelayMs(this.options.retryBackoff, state.consecutiveFailures);
    this.logger.error(
      { err: error, partition, offset, attempt: state.consecutiveFailures, retryDelayMs },
      "relay publish failed; partition paused, offset not committed",
    );

    const partitions = [{ topic, partitions: [partition] }];
    this.consumer.pause(partitions);
    const timer = setTimeout(() => {
      this.resumeTimers.delete(timer);
      if (this.stopping.signal.aborted) return;
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

  private partitionState(partition: number): PartitionState {
    let state = this.partitions.get(partition);
    if (state === undefined) {
      // Only reachable if a message arrives without an assignment callback;
      // without a known committed offset, continuity cannot be checked.
      state = { committedOffset: null, firstMessageChecked: true, consecutiveFailures: 0 };
      this.partitions.set(partition, state);
    }
    return state;
  }
}

// The client reports "no committed offset" as a negative offset, and also
// turns a committed offset of 0 into null. The relay never commits 0 (it
// commits the offset after a processed message), so both mean "none".
function parseCommittedOffset(offset: string | null): bigint | null {
  if (offset === null) return null;
  const value = BigInt(offset);
  return value < 0n ? null : value;
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
