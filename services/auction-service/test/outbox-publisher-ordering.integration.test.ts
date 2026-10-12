import { parseAuctionClosedV1, parseBidAcceptedV1 } from "@auction-bonanza/contracts";
import { describe, expect, it } from "vitest";

import { PostgresFinalizationRepository } from "../src/auctions/finalization.repository.js";
import { wrapPool } from "../src/database.js";
import { OUTBOX_LOG_MESSAGES } from "../src/outbox/outbox.publisher.js";
import { eventIdOf, messagesFor } from "./support/kafka.js";
import {
  byText,
  captureLogger,
  Gate,
  ScriptedTransport,
  waitFor,
} from "./support/outbox-fixtures.js";
import {
  infrastructureMissing,
  OutboxPublisherHarness,
  RETRY_DELAY_MS,
} from "./support/outbox-publisher-harness.js";

// What the publisher sends, and in which order, when claims compete or fail.
describe.skipIf(infrastructureMissing)(
  "outbox publisher ordering with PostgreSQL and Kafka",
  { timeout: 60_000 },
  () => {
    const harness = OutboxPublisherHarness.register();

    it("publishes the AuctionClosed.v1 event after the bid events of its auction", async () => {
      const { fixtures, topics, topic, pool } = harness;
      const auctionId = await fixtures.openAuction(harness.ownerId);
      const bidEventId = await fixtures.acceptBid(auctionId, harness.bidderId);
      await fixtures.expire(auctionId);
      await new PostgresFinalizationRepository(wrapPool(pool)).finalizeNextDue();
      const closedEventId = (
        await pool.query<{ event_id: string }>(
          "SELECT event_id::text FROM outbox_events WHERE auction_id = $1 AND event_type = 'AuctionClosed.v1'",
          [auctionId],
        )
      ).rows[0]!.event_id;
      const outboxPublisher = harness.publisher();

      await outboxPublisher.publishBatch();
      await outboxPublisher.publishBatch();

      const messages = await topics.readAll(topic);
      expect(messages.map(eventIdOf)).toEqual([bidEventId, closedEventId]);
      expect(messages[1]!.key).toBe(auctionId);
      expect(messages[1]!.headers["event-type"]).toBe("AuctionClosed.v1");
      expect(parseAuctionClosedV1(JSON.parse(messages[1]!.value!))).toMatchObject({
        auctionId,
        winnerId: harness.bidderId,
        finalAmountMinor: "1000",
      });
    });

    it("publishes the stored BidAccepted.v1 payload keyed by auction ID after the acknowledgement", async () => {
      const { fixtures, topics, topic } = harness;
      const auctionId = await fixtures.openAuction(harness.ownerId);
      const first = await fixtures.acceptBid(auctionId, harness.bidderId);
      const second = await fixtures.acceptBid(auctionId, harness.bidderId);
      const outboxPublisher = harness.publisher();

      // Only the earliest unpublished event of an auction is claimable.
      await expect(outboxPublisher.publishBatch()).resolves.toEqual({
        claimed: 1,
        published: 1,
        failed: 0,
      });
      expect((await fixtures.outboxRow(first)).publishedAt).toBeInstanceOf(Date);
      expect((await fixtures.outboxRow(second)).publishedAt).toBeNull();

      await expect(outboxPublisher.publishBatch()).resolves.toMatchObject({ published: 1 });
      await expect(outboxPublisher.publishBatch()).resolves.toEqual({
        claimed: 0,
        published: 0,
        failed: 0,
      });

      const rows = [await fixtures.outboxRow(first), await fixtures.outboxRow(second)];
      const messages = await topics.readAll(topic);
      expect(messages.map(eventIdOf)).toEqual([first, second]);
      expect(new Set(messages.map((message) => message.partition)).size).toBe(1);
      for (const [index, message] of messages.entries()) {
        const row = rows[index]!;
        expect(message.key).toBe(auctionId);
        expect(message.value).toBe(row.payloadJson);
        expect(message.headers).toEqual({
          "event-id": row.eventId,
          "event-type": "BidAccepted.v1",
          "content-type": "application/json",
        });
        expect(parseBidAcceptedV1(JSON.parse(message.value!))).toMatchObject({
          eventId: row.eventId,
          auctionId,
          auctionVersion: row.auctionVersion,
        });
        expect(row).toMatchObject({
          publishAttempts: 0,
          nextPublishAttemptAt: null,
          lastPublishError: null,
        });
      }
    });

    it("lets concurrent publishers claim disjoint auctions and never a claimed event's successor", async () => {
      const { fixtures, kafka, topics, topic, ownerId, bidderId } = harness;
      const [a, b, c, d] = await Promise.all(
        [1, 2, 3, 4].map(async () => fixtures.openAuction(ownerId)),
      );
      const a1 = await fixtures.acceptBid(a!, bidderId);
      const b1 = await fixtures.acceptBid(b!, bidderId);
      const c1 = await fixtures.acceptBid(c!, bidderId);
      const d1 = await fixtures.acceptBid(d!, bidderId);
      const a2 = await fixtures.acceptBid(a!, bidderId);
      const b2 = await fixtures.acceptBid(b!, bidderId);
      const c2 = await fixtures.acceptBid(c!, bidderId);

      // The first publisher claims the two oldest heads and is held after the
      // claim, with its row locks held, before Kafka is contacted.
      const gate = new Gate();
      const held = new ScriptedTransport(kafka);
      held.beforeSend = async () => gate.pass();
      const firstBatch = harness.publisher({ transport: held, batchSize: 2 }).publishBatch();
      await gate.arrivals(2);
      expect(held.attempted.map(eventIdOf).sort(byText)).toEqual([a1, b1].sort(byText));

      // A second publisher skips the locked heads (A1, B1) and must not treat
      // their successors (A2, B2) as heads. C2 waits behind its own claim, C1.
      const recording = new ScriptedTransport(kafka);
      const second = harness.publisher({ transport: recording });
      await expect(second.publishBatch()).resolves.toEqual({ claimed: 2, published: 2, failed: 0 });
      expect(recording.attempted.map(eventIdOf).sort(byText)).toEqual([c1, d1].sort(byText));

      gate.open();
      await expect(firstBatch).resolves.toEqual({ claimed: 2, published: 2, failed: 0 });
      await expect(second.publishBatch()).resolves.toEqual({ claimed: 3, published: 3, failed: 0 });

      const messages = await topics.readAll(topic);
      expect(messages.map(eventIdOf).sort(byText)).toEqual(
        [a1, b1, c1, d1, a2, b2, c2].sort(byText),
      );
      for (const [auctionId, expected] of [
        [a, [a1, a2]],
        [b, [b1, b2]],
        [c, [c1, c2]],
      ] as const) {
        expect(messagesFor(messages, auctionId!).map(eventIdOf)).toEqual(expected);
      }
    });

    it("publishes every event exactly once, in version order per auction, with many concurrent publishers", async () => {
      const { fixtures, topics, topic } = harness;
      const auctionIds = await Promise.all(
        Array.from({ length: 6 }, async () => fixtures.openAuction(harness.ownerId)),
      );
      const expected = new Map<string, string[]>(auctionIds.map((id) => [id, []]));
      for (let round = 0; round < 6; round += 1) {
        for (const auctionId of auctionIds) {
          expected.get(auctionId)!.push(await fixtures.acceptBid(auctionId, harness.bidderId));
        }
      }

      const shutdown = new AbortController();
      const runs = Array.from({ length: 4 }, () =>
        harness.publisher({ batchSize: 2 }).run(shutdown.signal),
      );
      await waitFor(async () => (await harness.unpublishedCount()) === 0, 30_000);
      shutdown.abort();
      await Promise.all(runs);

      const messages = await topics.readAll(topic);
      expect(messages).toHaveLength(36);
      expect(new Set(messages.map(eventIdOf)).size).toBe(36);
      for (const [auctionId, eventIds] of expected) {
        const auctionMessages = messagesFor(messages, auctionId);
        expect(new Set(auctionMessages.map((message) => message.partition)).size).toBe(1);
        expect(auctionMessages.map(eventIdOf)).toEqual(eventIds);
      }
    });

    it("blocks an auction behind a locked predecessor while other auctions progress", async () => {
      const { fixtures, topics, topic, ownerId, bidderId } = harness;
      const blocked = await fixtures.openAuction(ownerId);
      const other = await fixtures.openAuction(ownerId);
      const blocked1 = await fixtures.acceptBid(blocked, bidderId);
      const blocked2 = await fixtures.acceptBid(blocked, bidderId);
      const other1 = await fixtures.acceptBid(other, bidderId);
      const outboxPublisher = harness.publisher();

      // Stand-in for a publisher that has claimed the head event and is
      // still waiting for Kafka.
      const lockHolder = await harness.pool.connect();
      try {
        await lockHolder.query("BEGIN");
        await lockHolder.query("SELECT 1 FROM outbox_events WHERE event_id = $1 FOR UPDATE", [
          blocked1,
        ]);

        await expect(outboxPublisher.publishBatch()).resolves.toEqual({
          claimed: 1,
          published: 1,
          failed: 0,
        });
        expect((await fixtures.outboxRow(other1)).publishedAt).toBeInstanceOf(Date);
        await expect(outboxPublisher.publishBatch()).resolves.toMatchObject({ claimed: 0 });
        expect((await fixtures.outboxRow(blocked2)).publishedAt).toBeNull();
      } finally {
        await lockHolder.query("ROLLBACK");
        lockHolder.release();
      }

      await expect(outboxPublisher.publishBatch()).resolves.toMatchObject({ published: 1 });
      await expect(outboxPublisher.publishBatch()).resolves.toMatchObject({ published: 1 });
      const messages = await topics.readAll(topic);
      expect(messagesFor(messages, blocked).map(eventIdOf)).toEqual([blocked1, blocked2]);
      expect(messagesFor(messages, other).map(eventIdOf)).toEqual([other1]);
    });

    it("blocks an auction behind a retrying predecessor and republishes it unchanged", async () => {
      const { fixtures, kafka, topics, topic, ownerId, bidderId } = harness;
      const failing = await fixtures.openAuction(ownerId);
      const healthy = await fixtures.openAuction(ownerId);
      const failing1 = await fixtures.acceptBid(failing, bidderId);
      const healthy1 = await fixtures.acceptBid(healthy, bidderId);
      const failing2 = await fixtures.acceptBid(failing, bidderId);
      const healthy2 = await fixtures.acceptBid(healthy, bidderId);
      const stored = await fixtures.outboxRow(failing1);

      const transport = new ScriptedTransport(kafka);
      let injectedFailures = 1;
      transport.beforeSend = (message) => {
        if (eventIdOf(message) === failing1 && injectedFailures > 0) {
          injectedFailures -= 1;
          throw Object.assign(new Error("Local: Broker transport failure"), { code: -195 });
        }
      };
      const { logger, entries } = captureLogger();
      const outboxPublisher = harness.publisher({ transport, logger });

      await expect(outboxPublisher.publishBatch()).resolves.toEqual({
        claimed: 2,
        published: 1,
        failed: 1,
      });
      const afterFailure = await fixtures.outboxRow(failing1);
      expect(afterFailure).toMatchObject({ publishedAt: null, publishAttempts: 1 });
      expect(afterFailure.lastPublishError).toBe("Error (-195): Local: Broker transport failure");
      // Equal-jitter backoff with random() = 0 schedules half of the ceiling.
      expect(afterFailure.nextPublishAttemptAt!.getTime() - Date.now()).toBeGreaterThan(
        RETRY_DELAY_MS / 2 - 10_000,
      );
      expect(entries).toContainEqual(
        expect.objectContaining({
          level: 50,
          msg: OUTBOX_LOG_MESSAGES.publicationFailed,
          eventId: failing1,
          eventType: "BidAccepted.v1",
          auctionId: failing,
          auctionVersion: stored.auctionVersion,
          attempt: 1,
          retryDelayMs: RETRY_DELAY_MS / 2,
          err: expect.objectContaining({ message: "Local: Broker transport failure" }),
        }),
      );

      // The healthy auction keeps flowing; the failing auction's successor
      // stays behind its waiting predecessor.
      await expect(outboxPublisher.publishBatch()).resolves.toEqual({
        claimed: 1,
        published: 1,
        failed: 0,
      });
      await expect(outboxPublisher.publishBatch()).resolves.toMatchObject({ claimed: 0 });
      expect((await fixtures.outboxRow(failing2)).publishedAt).toBeNull();

      await fixtures.makeRetriesDue();
      await expect(outboxPublisher.publishBatch()).resolves.toMatchObject({ published: 1 });
      await expect(outboxPublisher.publishBatch()).resolves.toMatchObject({ published: 1 });

      const republished = await fixtures.outboxRow(failing1);
      expect(republished).toMatchObject({
        eventId: stored.eventId,
        payloadJson: stored.payloadJson,
        publishAttempts: 1,
        nextPublishAttemptAt: null,
        lastPublishError: afterFailure.lastPublishError,
      });
      expect(republished.publishedAt).toBeInstanceOf(Date);

      const messages = await topics.readAll(topic);
      const failingMessages = messagesFor(messages, failing);
      expect(failingMessages.map(eventIdOf)).toEqual([failing1, failing2]);
      expect(failingMessages[0]!.value).toBe(stored.payloadJson);
      expect(messagesFor(messages, healthy).map(eventIdOf)).toEqual([healthy1, healthy2]);
    });
  },
);
