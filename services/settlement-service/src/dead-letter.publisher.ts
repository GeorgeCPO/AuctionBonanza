import { setTimeout as delay } from "node:timers/promises";

import { KafkaJS } from "@confluentinc/kafka-javascript";

import type { KafkaDeadLetterPublisherOptions } from "./dead-letter.publisher.types.js";
import type {
  DeadLetterHeaders,
  DeadLetterPublisher,
  DeadLetterRecord,
} from "./dead-letter.types.js";
import { createKafkaLogger } from "./shared/kafka-logger.js";
import { withTimeout } from "./shared/timeout.js";

// A topic created a moment ago may not be in the broker's metadata yet, so the
// startup check looks a few times before it calls the topic missing.
const TOPIC_CHECK_ATTEMPTS = 10;
const TOPIC_CHECK_DELAY_MS = 200;

// Produces unprocessable records to the dead-letter topic. The producer is
// configured like the outbox publisher's: a send resolves only once every
// in-sync replica has the record, because the caller commits the source
// offset right after, and the dead letter is then the only copy that matters.
export class KafkaDeadLetterPublisher implements DeadLetterPublisher {
  private readonly options: KafkaDeadLetterPublisherOptions;
  private readonly producer: KafkaJS.Producer;
  private connected = false;

  constructor(options: KafkaDeadLetterPublisherOptions) {
    this.options = options;
    this.producer = new KafkaJS.Kafka({}).producer({
      kafkaJS: { logger: createKafkaLogger(options.logger) },
      "bootstrap.servers": options.brokers.join(","),
      "client.id": options.clientId,
      // Wait for all in-sync replicas.
      acks: -1,
      // Broker-side de-duplication of the producer's own internal retries. It
      // does not de-duplicate a record sent again after a timeout or a crash:
      // that is a separate send, and the dead-letter topic is at-least-once.
      "enable.idempotence": true,
      // Upper bound on queueing plus all internal retries for one message.
      // publish() also races each send against its own timer, because
      // librdkafka scans for expired messages periodically, not exactly.
      "message.timeout.ms": options.sendTimeoutMs,
      "request.timeout.ms": Math.min(options.sendTimeoutMs, 30_000),
      "socket.connection.setup.timeout.ms": Math.min(options.sendTimeoutMs, 10_000),
      // Notice a recovered broker well within one send timeout.
      "reconnect.backoff.ms": 100,
      "reconnect.backoff.max.ms": 1_000,
      partitioner: "murmur2_random",
      "linger.ms": 5,
    });
  }

  // Connects, then requires the dead-letter topic to exist. Topics are
  // provisioned explicitly (automatic creation is off), so without the check a
  // missing or misspelled topic shows up only at the first record that cannot
  // be processed, as a send that times out and stalls its partition behind an
  // error that names no cause. Failing here fails the deploy instead.
  async connect(): Promise<void> {
    await this.producer.connect();
    this.connected = true;
    await this.requireTopic();
  }

  async publish(record: DeadLetterRecord): Promise<void> {
    const send = this.producer.send({
      topic: this.options.topic,
      messages: [
        {
          key: record.key,
          value: record.value,
          headers: toKafkaHeaders(record.headers),
        },
      ],
    });
    await withTimeout(send, this.options.sendTimeoutMs, "dead-letter send");
  }

  // Waits for queued deliveries, bounded by timeoutMs, then closes the client.
  // A producer that never connected has nothing to flush; flushing it would
  // only raise a state error that hides why startup failed.
  async disconnect(timeoutMs: number): Promise<void> {
    if (!this.connected) {
      return;
    }
    this.connected = false;
    try {
      await this.producer.flush({ timeout: timeoutMs });
    } finally {
      await this.producer.disconnect();
    }
  }

  private async requireTopic(): Promise<void> {
    const admin = new KafkaJS.Kafka({}).admin({
      kafkaJS: { logger: createKafkaLogger(this.options.logger) },
      "bootstrap.servers": this.options.brokers.join(","),
      "client.id": this.options.clientId,
    });
    await admin.connect();
    try {
      for (let attempt = 1; attempt <= TOPIC_CHECK_ATTEMPTS; attempt += 1) {
        const topics = await admin.listTopics();
        if (topics.includes(this.options.topic)) {
          return;
        }
        if (attempt < TOPIC_CHECK_ATTEMPTS) {
          await delay(TOPIC_CHECK_DELAY_MS);
        }
      }
      throw new Error(
        `dead-letter topic ${this.options.topic} does not exist; provision it (docker compose up kafka-init) or fix SETTLEMENT_DEAD_LETTER_TOPIC`,
      );
    } finally {
      await admin.disconnect();
    }
  }
}

// The client's header type wants mutable arrays; ours are read-only.
function toKafkaHeaders(headers: DeadLetterHeaders): KafkaJS.IHeaders {
  const result: KafkaJS.IHeaders = {};
  for (const [name, value] of Object.entries(headers)) {
    result[name] = typeof value === "string" || Buffer.isBuffer(value) ? value : [...value];
  }
  return result;
}
