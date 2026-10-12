import { randomUUID } from "node:crypto";
import { Writable } from "node:stream";
import { setTimeout as delay } from "node:timers/promises";

import { KafkaJS } from "@confluentinc/kafka-javascript";
import { pino, type Logger } from "pino";
import { createCluster } from "redis";

import { auctionChannel } from "../../src/bus/bus.js";
import type { BusPublisher } from "../../src/bus/bus.types.js";
import { EventRelay } from "../../src/relay/relay.js";

const silentKafkaLogger: KafkaJS.Logger = {
  info: () => {},
  debug: () => {},
  warn: () => {},
  error: () => {},
  namespace: () => silentKafkaLogger,
  setLogLevel: () => {},
};

export interface ProducedRecord {
  partition: number;
  offset: bigint;
}

export interface TestRecord {
  key: string | null;
  value: string | null;
  // Omitted or undefined: the record has no event-type header.
  eventType?: string | undefined;
}

// A valid BidAccepted.v1 record for one auction version.
export function bidAccepted(auctionId: string, version: number): TestRecord {
  const eventId = randomUUID();
  return {
    key: auctionId,
    eventType: "BidAccepted.v1",
    value: JSON.stringify({
      eventId,
      eventType: "BidAccepted.v1",
      occurredAt: "2030-01-01T10:00:00.000000Z",
      auctionId,
      auctionVersion: String(version),
      bidId: String(1000 + version),
      amountMinor: String(100 * version),
      currencyCode: "USD",
    }),
  };
}

export async function waitFor(
  condition: () => boolean,
  description: string,
  timeoutMs = 20_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for ${description}`);
    }
    await delay(25);
  }
}

// Collects log lines so tests can assert on what the relay reported.
export function captureLogger(): { logger: Logger; lines: Record<string, unknown>[] } {
  const lines: Record<string, unknown>[] = [];
  const stream = new Writable({
    write(chunk: Buffer, _encoding, callback) {
      lines.push(JSON.parse(chunk.toString("utf8")) as Record<string, unknown>);
      callback();
    },
  });
  return { logger: pino({ level: "debug" }, stream), lines };
}

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
    await this.admin.connect();
    await this.producer.connect();
  }

  async create(partitions: number): Promise<string> {
    const topic = `relay-test-${randomUUID()}`;
    await this.admin.createTopics({
      topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
    });
    this.topics.add(topic);
    return topic;
  }

  newGroup(): string {
    const group = `relay-test-${randomUUID()}`;
    this.groups.add(group);
    return group;
  }

  // Produces records in order and returns where each one landed.
  async produce(topic: string, records: readonly TestRecord[]): Promise<ProducedRecord[]> {
    const produced: ProducedRecord[] = [];
    for (const record of records) {
      const [metadata] = await this.producer.send({
        topic,
        messages: [
          {
            key: record.key,
            value: record.value,
            headers: record.eventType === undefined ? {} : { "event-type": record.eventType },
          },
        ],
      });
      produced.push({
        partition: metadata!.partition,
        offset: BigInt(metadata!.baseOffset ?? metadata!.offset!),
      });
    }
    return produced;
  }

  async committedOffsets(groupId: string, topic: string): Promise<Map<number, bigint>> {
    const [result] = await this.admin.fetchOffsets({ groupId, topics: [topic] });
    return new Map(
      (result?.partitions ?? []).map(({ partition, offset }) => [partition, BigInt(offset)]),
    );
  }

  // The group's committed offset for every partition that holds records.
  // A partition the group never received a record from has no commit.
  async committedOffsetsOfNonEmptyPartitions(
    groupId: string,
    topic: string,
  ): Promise<Map<number, bigint | undefined>> {
    const committed = await this.committedOffsets(groupId, topic);
    const ends = await this.endOffsets(topic);
    return new Map(
      [...ends]
        .filter(([, end]) => end > 0n)
        .map(([partition]) => [partition, committed.get(partition)]),
    );
  }

  async nonEmptyEndOffsets(topic: string): Promise<Map<number, bigint>> {
    return new Map([...(await this.endOffsets(topic))].filter(([, end]) => end > 0n));
  }

  async endOffsets(topic: string): Promise<Map<number, bigint>> {
    const offsets = await this.admin.fetchTopicOffsets(topic);
    return new Map(offsets.map(({ partition, high }) => [partition, BigInt(high)]));
  }

  // Simulates retention: records below the offset are deleted.
  async deleteRecordsBefore(topic: string, partition: number, offset: bigint): Promise<void> {
    await this.admin.deleteTopicRecords({
      topic,
      partitions: [{ partition, offset: offset.toString() }],
    });
  }

  async disconnect(): Promise<void> {
    if (this.groups.size > 0) {
      await this.admin.deleteGroups([...this.groups]).catch(() => {});
    }
    if (this.topics.size > 0) {
      await this.admin.deleteTopics({ topics: [...this.topics] });
    }
    await this.producer.disconnect();
    await this.admin.disconnect();
  }
}

export interface BusMessage {
  channel: string;
  message: string;
}

// Subscribes through the cluster client, the simplest way to follow a few
// sharded channels in tests. The edge will manage its subscriptions itself.
export class BusRecorder {
  readonly messages: BusMessage[] = [];
  private readonly cluster: ReturnType<typeof createCluster>;

  constructor(nodes: readonly string[]) {
    this.cluster = createCluster({ rootNodes: nodes.map((node) => ({ url: `redis://${node}` })) });
    this.cluster.on("error", () => {});
  }

  async connect(): Promise<void> {
    await this.cluster.connect();
  }

  async subscribe(...channels: string[]): Promise<void> {
    for (const channel of channels) {
      await this.cluster.sSubscribe(channel, (message) => {
        this.messages.push({ channel, message });
      });
    }
  }

  // The auction versions received on an auction's channel, in arrival order.
  versionsOf(auctionId: string): string[] {
    return this.messages
      .filter(({ channel }) => channel === auctionChannel(auctionId))
      .map(({ message }) => {
        const envelope = JSON.parse(message) as { event: { auctionVersion: string } };
        return envelope.event.auctionVersion;
      });
  }

  async close(): Promise<void> {
    await this.cluster.close();
  }
}

// Records every publish, in order, before handing it to the real publisher.
// `failWhile` makes matching publishes reject without reaching the bus.
export class ScriptedPublisher implements BusPublisher {
  readonly published: BusMessage[] = [];
  readonly failures: BusMessage[] = [];
  failWhile: (channel: string, message: string) => boolean = () => false;

  constructor(private readonly inner: BusPublisher) {}

  async publish(channel: string, message: string): Promise<void> {
    if (this.failWhile(channel, message)) {
      this.failures.push({ channel, message });
      throw new Error(`scripted publish failure on ${channel}`);
    }
    await this.inner.publish(channel, message);
    this.published.push({ channel, message });
  }
}

export interface RelayRun {
  relay: EventRelay;
  stop(): Promise<void>;
}

export interface StartRelayOptions {
  brokers: string;
  topic: string;
  groupId: string;
  publisher: BusPublisher;
  logger?: Logger;
}

export async function startRelay(options: StartRelayOptions): Promise<RelayRun> {
  const relay = new EventRelay({
    brokers: [options.brokers],
    clientId: `relay-test-${randomUUID()}`,
    groupId: options.groupId,
    topic: options.topic,
    publisher: options.publisher,
    partitionConcurrency: 4,
    commitIntervalMs: 100,
    sessionTimeoutMs: 6_000,
    retryBackoff: { baseDelayMs: 50, maxDelayMs: 200 },
    logger: options.logger ?? pino({ level: "silent" }),
  });
  await relay.start();
  let stopped = false;
  return {
    relay,
    stop: async () => {
      if (stopped) {
        return;
      }
      stopped = true;
      await relay.stop();
    },
  };
}
