import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import { parseBidAcceptedV1 } from "@auction-bonanza/contracts";
import { KafkaJS } from "@confluentinc/kafka-javascript";

import { countMessages, fetchSettledOffsets } from "./topic-offsets.js";

export interface ConsumedMessage {
  partition: number;
  offset: bigint;
  key: string | null;
  value: string | null;
  headers: Record<string, string>;
}

const silentLogger: KafkaJS.Logger = {
  info: () => {},
  debug: () => {},
  warn: () => {},
  error: () => {},
  namespace: () => silentLogger,
  setLogLevel: () => {},
};

export class KafkaTestTopics {
  private readonly kafka = new KafkaJS.Kafka({});
  private readonly admin: KafkaJS.Admin;
  private readonly topics = new Set<string>();

  constructor(private readonly brokers: string) {
    this.admin = this.kafka.admin({
      kafkaJS: { logger: silentLogger },
      "bootstrap.servers": brokers,
    });
  }

  async connect(): Promise<void> {
    await this.admin.connect();
  }

  async create(partitions = 6): Promise<string> {
    const topic = `outbox-test-${randomUUID()}`;
    await this.admin.createTopics({
      topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
    });
    this.topics.add(topic);
    return topic;
  }

  // Reads every message currently in the topic. The expected count comes from
  // the partition high watermarks, so the result is exact: no waiting period
  // is needed to be confident that no duplicate is still missing. The count
  // is taken only from an answer in which every partition reports real
  // offsets (see isSettled).
  async readAll(topic: string): Promise<ConsumedMessage[]> {
    const offsets = await fetchSettledOffsets(async () => this.admin.fetchTopicOffsets(topic), {
      topic,
      timeoutMs: 10_000,
      pollMs: 20,
    });
    const total = countMessages(offsets);
    if (total === 0) {
      return [];
    }

    const messages: ConsumedMessage[] = [];
    const consumer = this.kafka.consumer({
      kafkaJS: {
        groupId: `outbox-test-${randomUUID()}`,
        fromBeginning: true,
        logger: silentLogger,
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
            key: message.key?.toString("utf8") ?? null,
            value: message.value?.toString("utf8") ?? null,
            headers: Object.fromEntries(
              Object.entries(message.headers ?? {}).map(([name, value]) => [
                name,
                String(Array.isArray(value) ? value[0] : value),
              ]),
            ),
          });
        },
      });
      const deadline = Date.now() + 30_000;
      while (messages.length < total) {
        if (Date.now() > deadline) {
          throw new Error(`Read ${messages.length} of ${total} messages from ${topic}`);
        }
        await delay(50);
      }
    } finally {
      await consumer.disconnect();
    }

    return messages.sort((left, right) =>
      left.partition === right.partition
        ? Number(left.offset - right.offset)
        : left.partition - right.partition,
    );
  }

  async cleanup(): Promise<void> {
    if (this.topics.size > 0) {
      await this.admin.deleteTopics({ topics: [...this.topics] });
      this.topics.clear();
    }
  }

  async disconnect(): Promise<void> {
    await this.cleanup();
    await this.admin.disconnect();
  }
}

// Works for both consumed messages and the outbound messages a transport saw.
export function eventIdOf(message: { readonly headers: Readonly<Record<string, string>> }): string {
  const eventId = message.headers["event-id"];
  if (eventId === undefined) {
    throw new Error("Message has no event-id header");
  }
  return eventId;
}

export function versionOf(message: ConsumedMessage): string {
  return parseBidAcceptedV1(JSON.parse(message.value!)).auctionVersion;
}

// Messages for one auction, in partition (offset) order.
export function messagesFor(messages: ConsumedMessage[], auctionId: string): ConsumedMessage[] {
  return messages.filter((message) => message.key === auctionId);
}
