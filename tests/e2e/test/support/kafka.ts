import { randomUUID } from "node:crypto";

import { KafkaJS } from "@confluentinc/kafka-javascript";

const silentLogger: KafkaJS.Logger = {
  info: () => {},
  debug: () => {},
  warn: () => {},
  error: () => {},
  namespace: () => silentLogger,
  setLogLevel: () => {},
};

// A topic and a consumer group that belong to one test run, so the suite
// neither reads nor disturbs auction.events.
export class TestTopic {
  readonly name = `e2e-${randomUUID()}`;
  readonly groupId = `e2e-${randomUUID()}`;
  private readonly admin: KafkaJS.Admin;

  constructor(brokers: string) {
    this.admin = new KafkaJS.Kafka({}).admin({
      kafkaJS: { logger: silentLogger },
      "bootstrap.servers": brokers,
    });
  }

  async create(partitions: number): Promise<void> {
    await this.admin.connect();
    await this.admin.createTopics({
      topics: [{ topic: this.name, numPartitions: partitions, replicationFactor: 1 }],
    });
  }

  // Call after every consumer of the group stopped.
  async remove(): Promise<void> {
    await this.admin.deleteGroups([this.groupId]).catch(() => {});
    await this.admin.deleteTopics({ topics: [this.name] });
    await this.admin.disconnect();
  }
}
