import { randomUUID } from "node:crypto";

import { pino } from "pino";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { KafkaDeadLetterPublisher } from "../../src/dead-letter.publisher.js";
import { requireEnvironment } from "../support/environment.js";
import { KafkaTestTopics } from "../support/kafka-harness.js";

const brokers = requireEnvironment("TEST_KAFKA_BROKERS");

// Topics are provisioned explicitly, so a worker that starts against a topic
// that does not exist must say so at startup, not at the first poison record.
describe("dead-letter publisher startup", { timeout: 60_000 }, () => {
  let topics: KafkaTestTopics;

  beforeAll(async () => {
    topics = new KafkaTestTopics(brokers);
    await topics.connect();
  });

  afterAll(async () => {
    await topics?.disconnect();
  });

  function publisherFor(topic: string): KafkaDeadLetterPublisher {
    return new KafkaDeadLetterPublisher({
      brokers: [brokers],
      clientId: `settlement-test-${randomUUID()}`,
      topic,
      sendTimeoutMs: 5_000,
      logger: pino({ level: "silent" }),
    });
  }

  it("connects when the topic exists, even one created a moment ago", async () => {
    const publisher = publisherFor(await topics.create(1));

    try {
      await expect(publisher.connect()).resolves.toBeUndefined();
    } finally {
      await publisher.disconnect(5_000);
    }
  });

  it("refuses to connect when the topic does not exist, and names it", async () => {
    const missing = `settlement-test-missing-${randomUUID()}`;
    const publisher = publisherFor(missing);

    try {
      await expect(publisher.connect()).rejects.toThrow(
        `dead-letter topic ${missing} does not exist`,
      );
    } finally {
      // The producer had connected before the check, and must still be closed.
      await publisher.disconnect(5_000);
    }
  });
});
