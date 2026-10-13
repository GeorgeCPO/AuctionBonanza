import { pino } from "pino";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { KafkaDeadLetterPublisher } from "../../src/dead-letter.publisher.js";

// The Kafka client is replaced, so the publisher's own logic runs without a
// broker: the startup check for the topic, the send timeout, and shutdown.
const kafka = vi.hoisted(() => ({
  producer: {
    connect: vi.fn<() => Promise<void>>(),
    send: vi.fn<(message: unknown) => Promise<unknown>>(),
    flush: vi.fn<(options: { timeout: number }) => Promise<void>>(),
    disconnect: vi.fn<() => Promise<void>>(),
  },
  admin: {
    connect: vi.fn<() => Promise<void>>(),
    listTopics: vi.fn<() => Promise<string[]>>(),
    disconnect: vi.fn<() => Promise<void>>(),
  },
}));

vi.mock("@confluentinc/kafka-javascript", () => ({
  KafkaJS: {
    Kafka: class {
      producer() {
        return kafka.producer;
      }
      admin() {
        return kafka.admin;
      }
    },
  },
}));

const TOPIC = "auction.events.dlq";

function newPublisher(): KafkaDeadLetterPublisher {
  return new KafkaDeadLetterPublisher({
    brokers: ["localhost:9092"],
    clientId: "settlement-test",
    topic: TOPIC,
    sendTimeoutMs: 5_000,
    logger: pino({ level: "silent" }),
  });
}

describe("KafkaDeadLetterPublisher", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    for (const mock of [...Object.values(kafka.producer), ...Object.values(kafka.admin)]) {
      mock.mockReset();
      mock.mockResolvedValue(undefined);
    }
    kafka.admin.listTopics.mockResolvedValue([TOPIC]);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("connect", () => {
    it("connects the producer and accepts a topic that exists", async () => {
      await newPublisher().connect();

      expect(kafka.producer.connect).toHaveBeenCalledOnce();
      expect(kafka.admin.listTopics).toHaveBeenCalledOnce();
      expect(kafka.admin.disconnect).toHaveBeenCalledOnce();
    });

    it("waits for a topic that was created a moment ago to appear", async () => {
      kafka.admin.listTopics
        .mockResolvedValueOnce(["other"])
        .mockResolvedValueOnce(["other"])
        .mockResolvedValueOnce(["other", TOPIC]);

      const connected = newPublisher().connect();
      await vi.advanceTimersByTimeAsync(400);
      await connected;

      expect(kafka.admin.listTopics).toHaveBeenCalledTimes(3);
      expect(kafka.admin.disconnect).toHaveBeenCalledOnce();
    });

    it("refuses a topic that never appears, naming it and the way to provision it", async () => {
      kafka.admin.listTopics.mockResolvedValue(["other"]);

      const outcome = newPublisher()
        .connect()
        .catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(2_000);

      expect(await outcome).toEqual(
        new Error(
          `dead-letter topic ${TOPIC} does not exist; provision it (docker compose up kafka-init) or fix SETTLEMENT_DEAD_LETTER_TOPIC`,
        ),
      );
      expect(kafka.admin.listTopics).toHaveBeenCalledTimes(10);
      expect(kafka.admin.disconnect).toHaveBeenCalledOnce();
    });

    it("still closes the producer after refusing, because it had connected", async () => {
      kafka.admin.listTopics.mockResolvedValue([]);
      const publisher = newPublisher();
      const refused = publisher.connect().catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(2_000);
      await refused;

      await publisher.disconnect(1_000);

      expect(kafka.producer.flush).toHaveBeenCalledWith({ timeout: 1_000 });
      expect(kafka.producer.disconnect).toHaveBeenCalledOnce();
    });
  });

  describe("publish", () => {
    it("sends the record to the topic with its key, value and headers", async () => {
      const key = Buffer.from("42");
      const value = Buffer.from([0xff, 0x00]);
      kafka.producer.send.mockResolvedValue([{ partition: 0 }]);

      await newPublisher().publish({
        key,
        value,
        headers: { "dlq-reason": "invalid-json", repeated: [Buffer.from("a"), "b"] },
      });

      expect(kafka.producer.send).toHaveBeenCalledWith({
        topic: TOPIC,
        messages: [
          {
            key,
            value,
            headers: { "dlq-reason": "invalid-json", repeated: [Buffer.from("a"), "b"] },
          },
        ],
      });
    });

    it("gives up on a send the broker never acknowledges", async () => {
      kafka.producer.send.mockReturnValue(new Promise(() => {}));

      const outcome = newPublisher()
        .publish({ key: null, value: null, headers: {} })
        .catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(5_000);

      expect(await outcome).toEqual(new Error("dead-letter send timed out after 5000ms"));
    });

    it("passes on a send the broker refuses", async () => {
      const refused = new Error("Broker: Unknown topic or partition");
      kafka.producer.send.mockRejectedValue(refused);

      await expect(newPublisher().publish({ key: null, value: null, headers: {} })).rejects.toBe(
        refused,
      );
    });
  });

  describe("disconnect", () => {
    it("does nothing for a producer that never connected", async () => {
      await newPublisher().disconnect(1_000);

      expect(kafka.producer.flush).not.toHaveBeenCalled();
      expect(kafka.producer.disconnect).not.toHaveBeenCalled();
    });

    it("flushes, then closes, and closes only once", async () => {
      const publisher = newPublisher();
      await publisher.connect();

      await publisher.disconnect(1_000);
      await publisher.disconnect(1_000);

      expect(kafka.producer.flush).toHaveBeenCalledOnce();
      expect(kafka.producer.disconnect).toHaveBeenCalledOnce();
    });

    it("closes the producer even when the flush fails", async () => {
      const publisher = newPublisher();
      await publisher.connect();
      kafka.producer.flush.mockRejectedValue(new Error("flush timed out"));

      await expect(publisher.disconnect(1_000)).rejects.toThrow("flush timed out");

      expect(kafka.producer.disconnect).toHaveBeenCalledOnce();
    });
  });
});
