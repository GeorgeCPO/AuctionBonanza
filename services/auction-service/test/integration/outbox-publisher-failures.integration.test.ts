import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";

import { describe, expect, it } from "vitest";

import type { Database } from "../../src/database.js";
import { PUBLISH_TIMEOUT_GRACE_MS } from "../../src/outbox/outbox.config.js";
import { OUTBOX_LOG_MESSAGES } from "../../src/outbox/outbox.publisher.js";
import { KafkaMessageTransport } from "../../src/outbox/outbox.transport.js";
import { KafkaFaultProxy } from "../support/kafka-fault-proxy.js";
import { eventIdOf, messagesFor, versionOf } from "../support/kafka.js";
import { captureLogger, Gate, ScriptedTransport, waitFor } from "../support/outbox-fixtures.js";
import {
  CYCLE_ALLOWANCE_MS,
  OutboxPublisherHarness,
  PUBLISH_TIMEOUT_MS,
  silentLogger,
} from "../support/outbox-publisher-harness.js";
import { requireEnvironment } from "../support/environment.js";

// host:port of the broker's FAULT_INJECTION listener (see docker-compose.yml).
const faultInjectionUpstream = requireEnvironment("TEST_KAFKA_FAULT_INJECTION_UPSTREAM");
// The FAULT_INJECTION listener advertises this port, where the test proxy runs.
const FAULT_PROXY_PORT = 19_092;

// What the publisher does when publication fails, stalls, or is interrupted:
// events are never lost, and duplicates arise only where documented.
describe("outbox publisher failure handling with PostgreSQL and Kafka", { timeout: 60_000 }, () => {
  const harness = OutboxPublisherHarness.register();

  it("never discards or marks published an event it cannot publish", async () => {
    const { fixtures, topics, topic } = harness;
    const auctionId = await fixtures.openAuction(harness.ownerId);
    const unknownEventId = await fixtures.insertUnpublishableEvent(auctionId);
    const successor = await fixtures.acceptBid(auctionId, harness.bidderId);
    const { logger, entries } = captureLogger();
    const outboxPublisher = harness.publisher({ logger });

    await expect(outboxPublisher.publishBatch()).resolves.toEqual({
      claimed: 1,
      published: 0,
      failed: 1,
    });
    await fixtures.makeRetriesDue();
    await expect(outboxPublisher.publishBatch()).resolves.toEqual({
      claimed: 1,
      published: 0,
      failed: 1,
    });

    expect(await fixtures.outboxRow(unknownEventId)).toMatchObject({
      publishedAt: null,
      publishAttempts: 2,
      lastPublishError: "UnpublishableEventError: No publication contract for Unknown.v1",
    });
    expect((await fixtures.outboxRow(successor)).publishedAt).toBeNull();
    expect(await topics.readAll(topic)).toEqual([]);
    expect(entries).toContainEqual(
      expect.objectContaining({
        level: 50,
        msg: OUTBOX_LOG_MESSAGES.unpublishable,
        eventId: unknownEventId,
        attempt: 2,
      }),
    );
  });

  it("bounds a send that never settles and republishes after a late acknowledgement", async () => {
    const { fixtures, kafka, topics, topic } = harness;
    const auctionId = await fixtures.openAuction(harness.ownerId);
    const eventId = await fixtures.acceptBid(auctionId, harness.bidderId);
    const stored = await fixtures.outboxRow(eventId);
    // The first send stalls before reaching Kafka until the gate opens,
    // standing in for a delivery report that arrives far too late.
    const gate = new Gate();
    const stalling = new ScriptedTransport(kafka);
    let stalls = 1;
    stalling.beforeSend = async () => {
      if (stalls > 0) {
        stalls -= 1;
        await gate.pass();
      }
    };
    const { logger, entries } = captureLogger();
    const outboxPublisher = harness.publisher({ transport: stalling, logger });

    const startedAt = performance.now();
    await expect(outboxPublisher.publishBatch()).resolves.toEqual({
      claimed: 1,
      published: 0,
      failed: 1,
    });
    const elapsed = performance.now() - startedAt;
    expect(elapsed).toBeGreaterThanOrEqual(PUBLISH_TIMEOUT_MS + PUBLISH_TIMEOUT_GRACE_MS);
    expect(elapsed).toBeLessThan(
      PUBLISH_TIMEOUT_MS + PUBLISH_TIMEOUT_GRACE_MS + CYCLE_ALLOWANCE_MS,
    );
    expect(await fixtures.outboxRow(eventId)).toMatchObject({
      publishedAt: null,
      publishAttempts: 1,
      lastPublishError: `PublishTimeoutError: Kafka did not acknowledge within ${
        PUBLISH_TIMEOUT_MS + PUBLISH_TIMEOUT_GRACE_MS
      }ms`,
    });

    // The abandoned send now completes. Kafka has the event while the
    // outbox still says unpublished, so the retry creates a duplicate.
    gate.open();
    await waitFor(
      async () => entries.some((entry) => entry.msg === OUTBOX_LOG_MESSAGES.lateAcknowledgement),
      10_000,
    );
    expect(entries).toContainEqual(
      expect.objectContaining({
        level: 40,
        msg: OUTBOX_LOG_MESSAGES.lateAcknowledgement,
        eventId,
      }),
    );

    await fixtures.makeRetriesDue();
    await expect(outboxPublisher.publishBatch()).resolves.toMatchObject({ published: 1 });
    const messages = await topics.readAll(topic);
    expect(messages.map(eventIdOf)).toEqual([eventId, eventId]);
    expect(messages.map((message) => message.value)).toEqual([
      stored.payloadJson,
      stored.payloadJson,
    ]);
  });

  it("fails within a bounded time while Kafka is unreachable and recovers without losing events", async () => {
    const { fixtures, topics, topic, ownerId, bidderId } = harness;
    const [upstreamHost, upstreamPort] = faultInjectionUpstream.split(":");
    const proxy = new KafkaFaultProxy(upstreamHost!, Number(upstreamPort));
    await proxy.listen(FAULT_PROXY_PORT);
    const proxiedKafka = new KafkaMessageTransport({
      brokers: [`localhost:${FAULT_PROXY_PORT}`],
      clientId: "outbox-publisher-fault-test",
      publishTimeoutMs: PUBLISH_TIMEOUT_MS,
      logger: silentLogger,
    });

    try {
      await proxiedKafka.connect();
      const first = await fixtures.openAuction(ownerId);
      const second = await fixtures.openAuction(ownerId);
      const first1 = await fixtures.acceptBid(first, bidderId);
      const { logger, entries } = captureLogger();
      const outboxPublisher = harness.publisher({ transport: proxiedKafka, logger });
      // The path through the proxy works before the outage.
      await expect(outboxPublisher.publishBatch()).resolves.toMatchObject({ published: 1 });

      proxy.cut();
      const first2 = await fixtures.acceptBid(first, bidderId);
      const second1 = await fixtures.acceptBid(second, bidderId);
      const storedPayloads = new Map([
        [first2, (await fixtures.outboxRow(first2)).payloadJson],
        [second1, (await fixtures.outboxRow(second1)).payloadJson],
      ]);

      const startedAt = performance.now();
      await expect(outboxPublisher.publishBatch()).resolves.toEqual({
        claimed: 2,
        published: 0,
        failed: 2,
      });
      // Loose sanity bound only; the exact timeout guarantee is covered
      // deterministically by the hung-send test.
      expect(performance.now() - startedAt).toBeLessThan(
        PUBLISH_TIMEOUT_MS + PUBLISH_TIMEOUT_GRACE_MS + CYCLE_ALLOWANCE_MS,
      );
      for (const eventId of [first2, second1]) {
        const row = await fixtures.outboxRow(eventId);
        expect(row).toMatchObject({ publishedAt: null, publishAttempts: 1 });
        expect(row.lastPublishError).toMatch(/timed out/i);
      }
      expect(
        entries.filter((entry) => entry.msg === OUTBOX_LOG_MESSAGES.publicationFailed),
      ).toHaveLength(2);

      // Recovery: keep cycling as the worker loop would. The first
      // attempts after the broker returns may still fail while the client
      // reconnects; each failure is recorded and retried, never dropped.
      proxy.restore();
      await waitFor(async () => {
        await fixtures.makeRetriesDue();
        await outboxPublisher.publishBatch();
        return (await harness.unpublishedCount()) === 0;
      }, 30_000);

      const messages = await topics.readAll(topic);
      expect(messagesFor(messages, first).map(eventIdOf)).toEqual([first1, first2]);
      expect(messagesFor(messages, second).map(eventIdOf)).toEqual([second1]);
      for (const message of messages.filter((candidate) =>
        storedPayloads.has(eventIdOf(candidate)),
      )) {
        expect(message.value).toBe(storedPayloads.get(eventIdOf(message)));
      }
    } finally {
      proxy.restore();
      await proxiedKafka.disconnect(1_000);
      await proxy.close();
    }
  });

  it("republishes an identical event after a crash between the Kafka acknowledgement and the commit", async () => {
    const { fixtures, kafka, topics, topic } = harness;
    const auctionId = await fixtures.openAuction(harness.ownerId);
    const first = await fixtures.acceptBid(auctionId, harness.bidderId);
    const second = await fixtures.acceptBid(auctionId, harness.bidderId);
    const stored = await fixtures.outboxRow(first);

    // Kill the publisher's database session right after Kafka acknowledges
    // and before published_at is written: the same observable outcome as the
    // process dying at that point.
    const crashingSession = `outbox-crash-${randomUUID()}`;
    const crashing = new ScriptedTransport(kafka);
    crashing.afterAcknowledgement = async () => {
      await harness.pool.query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1",
        [crashingSession],
      );
    };
    const { logger, entries } = captureLogger();
    await expect(
      harness
        .publisher({
          transport: crashing,
          database: harness.publisherDatabase(crashingSession),
          logger,
        })
        .publishBatch(),
    ).rejects.toThrow(/terminat/i);

    // Nothing was recorded, and the row lock died with the session.
    expect(await fixtures.outboxRow(first)).toEqual(stored);
    expect(entries).toContainEqual(
      expect.objectContaining({
        level: 40,
        msg: OUTBOX_LOG_MESSAGES.markerNotCommitted,
        eventIds: [first],
      }),
    );

    // A restarted publisher sends the same stored event again, then its
    // successor.
    const restarted = harness.publisher();
    await expect(restarted.publishBatch()).resolves.toMatchObject({ published: 1 });
    await expect(restarted.publishBatch()).resolves.toMatchObject({ published: 1 });

    const messages = messagesFor(await topics.readAll(topic), auctionId);
    expect(messages.map(eventIdOf)).toEqual([first, first, second]);
    const [original, duplicate] = messages;
    expect(duplicate!.value).toBe(original!.value);
    expect(duplicate!.value).toBe(stored.payloadJson);
    expect(duplicate!.key).toBe(original!.key);
    expect(duplicate!.headers).toEqual(original!.headers);
    expect(duplicate!.partition).toBe(original!.partition);
    // Raw versions on the partition repeat rather than strictly increase.
    expect(messages.map(versionOf)).toEqual([
      stored.auctionVersion,
      stored.auctionVersion,
      (await fixtures.outboxRow(second)).auctionVersion,
    ]);

    const afterRepublication = await fixtures.outboxRow(first);
    expect(afterRepublication).toMatchObject({
      eventId: stored.eventId,
      payloadJson: stored.payloadJson,
    });
    expect(afterRepublication.publishedAt).toBeInstanceOf(Date);
  });

  it("reports an unknown outcome and does not republish when the COMMIT reply is lost", async () => {
    const { fixtures, topics, topic } = harness;
    const auctionId = await fixtures.openAuction(harness.ownerId);
    const eventId = await fixtures.acceptBid(auctionId, harness.bidderId);

    // PostgreSQL really commits; the client only fails to hear about it,
    // as when the connection drops between the commit and its reply.
    const real = harness.publisherDatabase();
    const lostCommitReply: Database = {
      query: async (text, values) => real.query(text, values),
      connect: async () => {
        const client = await real.connect();
        return {
          query: async (text, values) => {
            const result = await client.query(text, values);
            if (text === "COMMIT") {
              throw new Error("Connection terminated unexpectedly");
            }
            return result;
          },
          release: (error) => client.release(error),
        };
      },
    };
    const { logger, entries } = captureLogger();
    await expect(
      harness.publisher({ database: lostCommitReply, logger }).publishBatch(),
    ).rejects.toThrow("Connection terminated unexpectedly");

    expect(entries).toContainEqual(
      expect.objectContaining({
        level: 40,
        msg: OUTBOX_LOG_MESSAGES.markerCommitUnknown,
        eventIds: [eventId],
      }),
    );
    // Here the commit did apply, so the event is not sent again.
    expect((await fixtures.outboxRow(eventId)).publishedAt).toBeInstanceOf(Date);
    await expect(harness.publisher().publishBatch()).resolves.toEqual({
      claimed: 0,
      published: 0,
      failed: 0,
    });
    expect((await topics.readAll(topic)).map(eventIdOf)).toEqual([eventId]);
  });

  it("finishes and commits the in-flight cycle before stopping", async () => {
    const { fixtures, kafka, topics, topic, ownerId, bidderId } = harness;
    const auctionId = await fixtures.openAuction(ownerId);
    const inFlight = await fixtures.acceptBid(auctionId, bidderId);
    const gate = new Gate();
    const held = new ScriptedTransport(kafka);
    held.beforeSend = async () => gate.pass();
    const shutdown = new AbortController();

    const running = harness.publisher({ transport: held }).run(shutdown.signal);
    await gate.arrivals(1);
    shutdown.abort();
    // Committed after shutdown was requested; must not start another cycle.
    const other = await fixtures.openAuction(ownerId);
    const notClaimed = await fixtures.acceptBid(other, bidderId);
    gate.open();
    await running;

    expect((await fixtures.outboxRow(inFlight)).publishedAt).toBeInstanceOf(Date);
    expect((await fixtures.outboxRow(notClaimed)).publishedAt).toBeNull();
    expect(held.attempted.map(eventIdOf)).toEqual([inFlight]);
    expect((await topics.readAll(topic)).map(eventIdOf)).toEqual([inFlight]);
  });
});
