import { randomUUID } from "node:crypto";

import type { AuctionClosedV1 } from "@auction-bonanza/contracts";
import { Pool } from "pg";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { DEAD_LETTER_HEADERS } from "../../src/dead-letter.record.js";
import { SETTLEMENT_LOG_MESSAGES } from "../../src/settlement.log-messages.js";
import { wrapPool } from "../../src/shared/database.js";
import { AuctionFixtures } from "../support/auction-fixtures.js";
import { requireEnvironment } from "../support/environment.js";
import { createIsolatedDatabase, type IsolatedDatabase } from "../support/isolated-database.js";
import {
  KafkaTestTopics,
  outboxRecord,
  waitFor,
  type TestRecord,
} from "../support/kafka-harness.js";
import { captureLogger } from "../support/log-capture.js";
import { startSettlement, type SettlementRun } from "../support/settlement-harness.js";

const brokers = requireEnvironment("TEST_KAFKA_BROKERS");
const adminDatabaseUrl = requireEnvironment("TEST_DATABASE_URL");

function bidRecord(auctionId: string, value?: string | null): TestRecord {
  const record = outboxRecord({ auctionId, eventId: randomUUID(), eventType: "BidAccepted.v1" });
  return value === undefined ? record : { ...record, value };
}

// The real consumer, handler, repository and dead-letter producer against real
// Kafka and PostgreSQL. Each test has its own topics and consumer group.
describe("settlement consumer", { timeout: 120_000 }, () => {
  let isolated: IsolatedDatabase;
  let pool: Pool;
  let fixtures: AuctionFixtures;
  let topics: KafkaTestTopics;
  let ownerId: string;
  let bidderId: string;
  const runs: SettlementRun[] = [];

  beforeAll(async () => {
    isolated = await createIsolatedDatabase(adminDatabaseUrl);
    pool = new Pool({ connectionString: isolated.url, max: 12 });
    fixtures = new AuctionFixtures(pool);
    topics = new KafkaTestTopics(brokers);
    await topics.connect();
  });

  beforeEach(async () => {
    await fixtures.reset();
    ownerId = await fixtures.user();
    bidderId = await fixtures.user();
  });

  afterEach(async () => {
    await Promise.all(runs.splice(0).map(async (run) => run.stop()));
  });

  afterAll(async () => {
    await topics?.disconnect();
    await pool?.end();
    await isolated?.drop();
  });

  async function setUp(partitions: number): Promise<{
    topic: string;
    deadLetterTopic: string;
    groupId: string;
  }> {
    const [topic, deadLetterTopic] = await Promise.all([
      topics.create(partitions),
      topics.create(1),
    ]);
    return { topic, deadLetterTopic, groupId: topics.newGroup() };
  }

  // A clean stop commits everything that was handled, whatever it was.
  async function expectEverythingCommitted(groupId: string, topic: string): Promise<void> {
    const { committed, ends } = await topics.commitProgress(groupId, topic);
    expect(committed).toEqual(ends);
  }

  async function start(
    setup: { topic: string; deadLetterTopic: string; groupId: string },
    options: { groupId?: string; logger?: ReturnType<typeof captureLogger>["logger"] } = {},
  ): Promise<SettlementRun> {
    const run = await startSettlement({
      brokers,
      topic: setup.topic,
      deadLetterTopic: setup.deadLetterTopic,
      groupId: options.groupId ?? setup.groupId,
      database: wrapPool(pool),
      ...(options.logger === undefined ? {} : { logger: options.logger }),
    });
    runs.push(run);
    return run;
  }

  async function waitForOrders(count: number): Promise<void> {
    await waitFor(async () => (await fixtures.orders()).length === count, `${count} orders`);
  }

  describe("the happy path", () => {
    it("creates an order per sold auction, records unsold ones, and ignores bids", async () => {
      const setup = await setUp(3);
      const sold = await fixtures.soldAuction(ownerId, bidderId, "1500");
      const unsold = await fixtures.unsoldAuction(ownerId);
      await topics.produce(setup.topic, [
        bidRecord(sold.event.auctionId),
        outboxRecord(sold.event),
        bidRecord(unsold.auctionId, "garbage that is not JSON"),
        outboxRecord(unsold),
        bidRecord(unsold.auctionId, null),
      ]);

      const run = await start(setup);
      await waitFor(
        async () => (await fixtures.processedEventIds()).length === 2,
        "both closed events processed",
      );
      await run.stop();

      expect(await fixtures.orders()).toEqual([
        expect.objectContaining({
          auctionId: sold.event.auctionId,
          buyerId: bidderId,
          amountMinor: "1500",
          sourceEventId: sold.event.eventId,
        }),
      ]);
      // Only the two closed events reached the database; bids are not settled
      // and, whatever they contain, are not dead-lettered.
      expect(run.repository.attempts).toHaveLength(2);
      expect(await topics.readAll(setup.deadLetterTopic)).toEqual([]);
      // Bids were handled too, though not settled.
      await expectEverythingCommitted(setup.groupId, setup.topic);
    });
  });

  describe("the same event delivered five times", () => {
    it("creates one order and acknowledges every copy", async () => {
      const setup = await setUp(3);
      const first = await fixtures.soldAuction(ownerId, bidderId, "1500");
      const second = await fixtures.soldAuction(ownerId, bidderId, "1600");
      const copy = outboxRecord(first.event);
      await topics.produce(setup.topic, [copy, copy, copy, copy, copy, outboxRecord(second.event)]);
      const { logger, lines } = captureLogger();

      const run = await start(setup, { logger });
      await waitForOrders(2);
      await waitFor(() => run.repository.attempts.length === 6, "all six deliveries");
      await run.stop();

      expect((await fixtures.orders()).map((order) => order.auctionId).sort()).toEqual(
        [first.event.auctionId, second.event.auctionId].sort(),
      );
      expect(lines.filter((line) => line.msg === SETTLEMENT_LOG_MESSAGES.duplicate)).toHaveLength(
        4,
      );
      expect(await topics.readAll(setup.deadLetterTopic)).toEqual([]);
      await expectEverythingCommitted(setup.groupId, setup.topic);
    });
  });

  describe("restarts and new consumer groups", () => {
    it("resumes after a clean restart without settling anything again", async () => {
      const setup = await setUp(3);
      const a = await fixtures.soldAuction(ownerId, bidderId, "1500");
      const b = await fixtures.soldAuction(ownerId, bidderId, "1600");
      const c = await fixtures.soldAuction(ownerId, bidderId, "1700");
      await topics.produce(setup.topic, [outboxRecord(a.event), outboxRecord(b.event)]);
      const first = await start(setup);
      await waitForOrders(2);
      await first.stop();

      await topics.produce(setup.topic, [outboxRecord(c.event)]);
      const second = await start(setup);
      await waitForOrders(3);

      // The restarted worker resumed from the committed offsets: it was
      // given the new event only.
      expect(second.repository.attempts.map((event) => event.eventId)).toEqual([c.event.eventId]);
    });

    it("lets a new group read the whole history and create no second order", async () => {
      const setup = await setUp(3);
      const a = await fixtures.soldAuction(ownerId, bidderId, "1500");
      const b = await fixtures.soldAuction(ownerId, bidderId, "1600");
      await topics.produce(setup.topic, [outboxRecord(a.event), outboxRecord(b.event)]);
      const original = await start(setup);
      await waitForOrders(2);
      await original.stop();
      const ordersBefore = await fixtures.orders();

      const replay = await start(setup, { groupId: topics.newGroup() });
      await waitFor(() => replay.repository.attempts.length === 2, "the history read again");
      await replay.stop();

      expect(await fixtures.orders()).toEqual(ordersBefore);
      expect(await fixtures.processedEventIds()).toHaveLength(2);
    });
  });

  describe("when the database is unavailable", () => {
    it("holds the partition at the failing record and settles everything in order afterwards", async () => {
      const setup = await setUp(1);
      const a = await fixtures.soldAuction(ownerId, bidderId, "1500");
      const b = await fixtures.soldAuction(ownerId, bidderId, "1600");
      const c = await fixtures.soldAuction(ownerId, bidderId, "1700");
      const produced = await topics.produce(setup.topic, [
        outboxRecord(a.event),
        outboxRecord(b.event),
        outboxRecord(c.event),
      ]);
      const run = await start(setup);
      run.repository.failWhile = (event) =>
        event.auctionId === b.event.auctionId ? new Error("database is down") : undefined;

      // a settles; b fails again and again; c waits behind b.
      await waitForOrders(1);
      const failuresOfB = () =>
        run.repository.attempts.filter((event) => event.eventId === b.event.eventId).length;
      await waitFor(() => failuresOfB() >= 3, "three failed attempts at the second record");
      // a's offset is committed, b's never is.
      await waitFor(
        async () =>
          (await topics.committedOffsets(setup.groupId, setup.topic)).get(0) ===
          produced[1]!.offset,
        "a's offset committed",
      );
      const failuresSoFar = failuresOfB();
      await waitFor(() => failuresOfB() >= failuresSoFar + 2, "two more failed attempts");
      expect((await topics.committedOffsets(setup.groupId, setup.topic)).get(0)).toBe(
        produced[1]!.offset,
      );
      expect(await fixtures.orders()).toHaveLength(1);
      expect(run.repository.attempts.some((event) => event.eventId === c.event.eventId)).toBe(
        false,
      );

      // The database comes back: b is settled, then c, in the order they were
      // produced.
      run.repository.failWhile = () => undefined;
      await waitForOrders(3);
      expect((await fixtures.orders()).map((order) => order.auctionId)).toEqual([
        a.event.auctionId,
        b.event.auctionId,
        c.event.auctionId,
      ]);
      await run.stop();
      expect(await topics.readAll(setup.deadLetterTopic)).toEqual([]);
      await expectEverythingCommitted(setup.groupId, setup.topic);
    });
  });

  describe("a record that can never be settled", () => {
    // Each case produces the bad record, then a good one on the same partition.
    const cases: {
      name: string;
      reason: string;
      bad: (event: AuctionClosedV1) => TestRecord;
    }[] = [
      {
        name: "is not JSON",
        reason: "invalid-json",
        bad: (event) => ({ ...outboxRecord(event), value: "{not json" }),
      },
      {
        name: "is not valid UTF-8",
        reason: "invalid-json",
        bad: (event) => ({ ...outboxRecord(event), value: Buffer.from([0xff, 0xfe, 0x00, 0x80]) }),
      },
      {
        name: "breaks the contract",
        reason: "contract-violation",
        bad: (event) => ({
          ...outboxRecord(event),
          value: JSON.stringify({ ...event, currencyCode: undefined }),
        }),
      },
      {
        name: "has no value",
        reason: "missing-value",
        bad: (event) => ({ ...outboxRecord(event), value: null }),
      },
      {
        name: "has no event-type header",
        reason: "missing-event-type",
        bad: (event) => ({
          ...outboxRecord(event),
          headers: { "event-id": event.eventId },
        }),
      },
      {
        name: "is keyed by another auction",
        reason: "key-mismatch",
        bad: (event) => ({ ...outboxRecord(event), key: "999999" }),
      },
      {
        name: "carries another event's id in its header",
        reason: "event-id-mismatch",
        bad: (event) => ({
          ...outboxRecord(event),
          headers: { "event-type": event.eventType, "event-id": randomUUID() },
        }),
      },
    ];

    it.each(cases)(
      "is dead-lettered unchanged when it $name, and later events still settle",
      async ({ reason, bad }) => {
        const setup = await setUp(1);
        const doomed = await fixtures.soldAuction(ownerId, bidderId, "1500");
        const good = await fixtures.soldAuction(ownerId, bidderId, "1600");
        const badRecord = bad(doomed.event);
        const [badProduced] = await topics.produce(setup.topic, [
          badRecord,
          outboxRecord(good.event),
        ]);

        const run = await start(setup);
        await waitForOrders(1);
        await run.stop();

        // Only the good event settled; the bad one is on the dead-letter topic.
        expect((await fixtures.orders()).map((order) => order.auctionId)).toEqual([
          good.event.auctionId,
        ]);
        const deadLetters = await topics.readAll(setup.deadLetterTopic);
        expect(deadLetters).toHaveLength(1);
        const [deadLetter] = deadLetters;
        expect(deadLetter?.key).toEqual(badRecord.key === null ? null : Buffer.from(badRecord.key));
        expect(deadLetter?.value).toEqual(
          badRecord.value === null ? null : Buffer.from(badRecord.value),
        );
        expect(deadLetter?.headers).toMatchObject({
          ...badRecord.headers,
          [DEAD_LETTER_HEADERS.reason]: reason,
          [DEAD_LETTER_HEADERS.sourceTopic]: setup.topic,
          [DEAD_LETTER_HEADERS.sourcePartition]: String(badProduced!.partition),
          [DEAD_LETTER_HEADERS.sourceOffset]: String(badProduced!.offset),
          [DEAD_LETTER_HEADERS.consumerGroup]: setup.groupId,
        });
        expect(Number.isNaN(Date.parse(deadLetter!.headers[DEAD_LETTER_HEADERS.failedAt]!))).toBe(
          false,
        );
        // The bad record's offset was committed: it does not come back.
        await expectEverythingCommitted(setup.groupId, setup.topic);
      },
    );
  });

  describe("an event the database refuses", () => {
    it("is dead-lettered with the rule that refused it, and rolled back completely", async () => {
      const setup = await setUp(1);
      const tampered = await fixtures.soldAuction(ownerId, bidderId, "1500");
      const good = await fixtures.soldAuction(ownerId, bidderId, "1600");
      await topics.produce(setup.topic, [
        outboxRecord({ ...tampered.event, finalAmountMinor: "1" } as AuctionClosedV1),
        outboxRecord(good.event),
      ]);

      const run = await start(setup);
      await waitForOrders(1);
      await run.stop();

      const [deadLetter] = await topics.readAll(setup.deadLetterTopic);
      expect(deadLetter?.headers).toMatchObject({
        [DEAD_LETTER_HEADERS.reason]: "database-rejected",
        [DEAD_LETTER_HEADERS.detail]: expect.stringContaining(
          "SQLSTATE 23514, constraint orders_terms_match_winning_bid",
        ),
      });
      // The refused event left no trace: its record of being handled was
      // rolled back with its order.
      expect(await fixtures.processedEventIds()).toEqual([good.event.eventId]);
    });

    it("is dead-lettered when it is a second, different event for a settled auction", async () => {
      const setup = await setUp(1);
      const sold = await fixtures.soldAuction(ownerId, bidderId, "1500");
      const secondEvent = { ...sold.event, eventId: randomUUID() };
      await topics.produce(setup.topic, [outboxRecord(sold.event), outboxRecord(secondEvent)]);

      const run = await start(setup);
      await waitFor(
        async () => (await topics.messageCount(setup.deadLetterTopic)) === 1,
        "a dead letter",
      );
      await run.stop();

      const [deadLetter] = await topics.readAll(setup.deadLetterTopic);
      expect(deadLetter?.headers[DEAD_LETTER_HEADERS.detail]).toContain(
        "constraint orders_auction_id_key",
      );
      expect(await fixtures.orders()).toHaveLength(1);
    });
  });

  describe("when the dead-letter topic is unavailable", () => {
    it("holds the record, loses nothing, and dead-letters it once the topic is back", async () => {
      const setup = await setUp(1);
      const doomed = await fixtures.soldAuction(ownerId, bidderId, "1500");
      const good = await fixtures.soldAuction(ownerId, bidderId, "1600");
      const [badProduced] = await topics.produce(setup.topic, [
        { ...outboxRecord(doomed.event), value: "{not json" },
        outboxRecord(good.event),
      ]);
      const run = await start(setup);
      run.deadLetters.failing = true;

      await waitFor(() => run.deadLetters.attempts.length >= 3, "three failed dead-letter sends");
      // The bad record blocks its partition: the good event behind it is not
      // settled, and the bad record's offset is not committed.
      expect(await fixtures.orders()).toEqual([]);
      const committed = (await topics.committedOffsets(setup.groupId, setup.topic)).get(0) ?? -1n;
      expect(committed).toBeLessThanOrEqual(badProduced!.offset);
      expect(await topics.readAll(setup.deadLetterTopic)).toEqual([]);

      run.deadLetters.failing = false;
      await waitForOrders(1);
      await run.stop();

      // Exactly one copy: the failed sends never reached the broker.
      expect(await topics.readAll(setup.deadLetterTopic)).toHaveLength(1);
      expect((await fixtures.orders())[0]?.auctionId).toBe(good.event.auctionId);
    });
  });
});
