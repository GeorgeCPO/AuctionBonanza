import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import { KafkaJS } from "@confluentinc/kafka-javascript";

import type { ProducedRecord, ReadRecord, TestRecord } from "./kafka-harness.types.js";
import { countMessages, fetchSettledOffsets } from "./topic-offsets.js";

const silentKafkaLogger: KafkaJS.Logger = {
  info: () => {},
  debug: () => {},
  warn: () => {},
  error: () => {},
  namespace: () => silentKafkaLogger,
  setLogLevel: () => {},
};

// A record as the outbox publisher sends it: key auctionId, the type and id
// headers, and the event as JSON.
export function outboxRecord(event: {
  auctionId: string;
  eventId: string;
  eventType: string;
}): TestRecord {
  return {
    key: event.auctionId,
    value: JSON.stringify(event),
    headers: {
      "event-id": event.eventId,
      "event-type": event.eventType,
      "content-type": "application/json",
    },
  };
}

// The description may be a function, so a message that quotes live state (such
// as a process's output) is built when the wait times out, not before it starts.
export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  description: string | (() => string),
  timeoutMs = 30_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    const now = Date.now();
    if (now > deadline) {
      const reason = typeof description === "string" ? description : description();
      throw new Error(`Timed out waiting for ${reason}`);
    }
    await delay(25);
  }
}

// Creates a topic and group per test, produces records, and reads topics back
// exactly: the expected count comes from the partitions' offsets, so no waiting
// period is needed to be confident that nothing is still on its way.
export class KafkaTestTopics {
  private readonly kafka = new KafkaJS.Kafka({});
  private readonly admin: KafkaJS.Admin;
  private readonly producer: KafkaJS.Producer;
  private readonly topics = new Set<string>();
  private readonly groups = new Set<string>();

  constructor(readonly brokers: string) {
    this.admin = this.kafka.admin({
      kafkaJS: { logger: silentKafkaLogger },
      "bootstrap.servers": brokers,
    });
    this.producer = this.kafka.producer({
      kafkaJS: { logger: silentKafkaLogger },
      "bootstrap.servers": brokers,
      acks: -1,
      partitioner: "murmur2_random",
    });
  }

  async connect(): Promise<void> {
    await Promise.all([this.admin.connect(), this.producer.connect()]);
  }

  async create(partitions: number): Promise<string> {
    const topic = `settlement-test-${randomUUID()}`;
    await this.admin.createTopics({
      topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
    });
    this.topics.add(topic);
    return topic;
  }

  newGroup(): string {
    const group = `settlement-test-${randomUUID()}`;
    this.groups.add(group);
    return group;
  }

  // Produces records in order and returns where each one landed.
  async produce(topic: string, records: readonly TestRecord[]): Promise<ProducedRecord[]> {
    const produced: ProducedRecord[] = [];
    for (const record of records) {
      const [metadata] = await this.producer.send({
        topic,
        messages: [{ key: record.key, value: record.value, headers: record.headers ?? {} }],
      });
      produced.push({
        partition: metadata!.partition,
        offset: BigInt(metadata!.baseOffset ?? metadata!.offset!),
      });
    }
    return produced;
  }

  // Every message currently in the topic, sorted by partition and offset.
  async readAll(topic: string): Promise<ReadRecord[]> {
    const total = await this.messageCount(topic);
    if (total === 0) {
      return [];
    }

    const messages: ReadRecord[] = [];
    const consumer = this.kafka.consumer({
      kafkaJS: {
        groupId: this.newGroup(),
        fromBeginning: true,
        logger: silentKafkaLogger,
      },
      "bootstrap.servers": this.brokers,
      "enable.auto.commit": false,
    });
    await consumer.connect();
    try {
      await consumer.subscribe({ topic });
      await consumer.run({
        eachMessage: async ({ partition, message }) => {
          messages.push({
            partition,
            offset: BigInt(message.offset),
            key: message.key,
            value: message.value,
            headers: Object.fromEntries(
              Object.entries(message.headers ?? {}).map(([name, value]) => {
                const first = Array.isArray(value) ? value[0] : value;
                return [name, first === undefined ? "" : first.toString()];
              }),
            ),
          });
        },
      });
      await waitFor(
        () => messages.length >= total,
        `${total} messages from ${topic} (read ${messages.length})`,
      );
    } finally {
      await consumer.disconnect();
    }

    return messages.sort((left, right) =>
      left.partition === right.partition
        ? Number(left.offset - right.offset)
        : left.partition - right.partition,
    );
  }

  async committedOffsets(groupId: string, topic: string): Promise<Map<number, bigint>> {
    const [result] = await this.admin.fetchOffsets({ groupId, topics: [topic] });
    return new Map(
      (result?.partitions ?? []).map(({ partition, offset }) => [partition, BigInt(offset)]),
    );
  }

  async messageCount(topic: string): Promise<number> {
    return countMessages(await this.settledOffsets(topic));
  }

  // How far a group has committed, next to where the topic ends, for every
  // partition that holds records. A partition the group never received a record
  // from has no commit. A group that handled everything has committed exactly
  // the end offsets: `expect(committed).toEqual(ends)`.
  async commitProgress(
    groupId: string,
    topic: string,
  ): Promise<{ committed: Map<number, bigint | undefined>; ends: Map<number, bigint> }> {
    const [committed, offsets] = await Promise.all([
      this.committedOffsets(groupId, topic),
      this.settledOffsets(topic),
    ]);
    const ends = new Map<number, bigint>();
    for (const { partition, high } of offsets) {
      const end = BigInt(high);
      if (end > 0n) {
        ends.set(partition, end);
      }
    }
    return {
      committed: new Map(
        [...ends.keys()].map((partition) => [partition, committed.get(partition)]),
      ),
      ends,
    };
  }

  async disconnect(): Promise<void> {
    await Promise.all([
      this.groups.size > 0 ? this.admin.deleteGroups([...this.groups]).catch(() => {}) : undefined,
      this.topics.size > 0 ? this.admin.deleteTopics({ topics: [...this.topics] }) : undefined,
      this.producer.disconnect(),
    ]);
    await this.admin.disconnect();
  }

  // Partitions of a freshly created topic can report offsets that are not
  // available yet; only a settled answer is a count (see topic-offsets.ts).
  private async settledOffsets(topic: string) {
    return fetchSettledOffsets(async () => this.admin.fetchTopicOffsets(topic), {
      topic,
      timeoutMs: 10_000,
      pollMs: 20,
    });
  }
}
