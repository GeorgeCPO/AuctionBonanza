import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { PermanentSettlementError } from "../../src/settlement.errors.js";
import { PostgresSettlementRepository } from "../../src/settlement.repository.js";
import { wrapPool, type Database } from "../../src/shared/database.js";
import { AuctionFixtures } from "../support/auction-fixtures.js";
import { requireEnvironment } from "../support/environment.js";
import { InterceptingDatabase } from "../support/intercepting-database.js";
import { createIsolatedDatabase, type IsolatedDatabase } from "../support/isolated-database.js";

const adminDatabaseUrl = requireEnvironment("TEST_DATABASE_URL");

// What settling an event does to real rows, and what survives the failures
// that matter at the acknowledgement boundary.
describe("settlement with PostgreSQL", () => {
  let isolated: IsolatedDatabase;
  let pool: Pool;
  let database: Database;
  let repository: PostgresSettlementRepository;
  let fixtures: AuctionFixtures;
  let ownerId: string;
  let bidderId: string;

  beforeAll(async () => {
    isolated = await createIsolatedDatabase(adminDatabaseUrl);
    pool = new Pool({ connectionString: isolated.url, max: 12 });
    database = wrapPool(pool);
    repository = new PostgresSettlementRepository(database);
    fixtures = new AuctionFixtures(pool);
  });

  beforeEach(async () => {
    await fixtures.reset();
    ownerId = await fixtures.user();
    bidderId = await fixtures.user();
  });

  afterAll(async () => {
    await pool.end();
    await isolated.drop();
  });

  describe("a sold auction", () => {
    it("creates one order for the winner and records the event", async () => {
      const { event, bidId } = await fixtures.soldAuction(ownerId, bidderId, "1500");

      const outcome = await repository.settle(event);

      expect(outcome).toEqual({ kind: "order_created", orderId: expect.any(String) });
      expect(await fixtures.orders()).toEqual([
        {
          id: (outcome as { orderId: string }).orderId,
          auctionId: event.auctionId,
          winningBidId: bidId,
          buyerId: bidderId,
          amountMinor: "1500",
          currencyCode: "EUR",
          sourceEventId: event.eventId,
        },
      ]);
      expect(await fixtures.processedEventIds()).toEqual([event.eventId]);
    });

    it("settles each of several auctions into its own order", async () => {
      const first = await fixtures.soldAuction(ownerId, bidderId, "1500");
      const second = await fixtures.soldAuction(ownerId, bidderId, "1600");

      await repository.settle(first.event);
      await repository.settle(second.event);

      expect((await fixtures.orders()).map((order) => order.amountMinor)).toEqual(["1500", "1600"]);
    });
  });

  describe("an unsold auction", () => {
    it("records the event and creates no order", async () => {
      const event = await fixtures.unsoldAuction(ownerId);

      const outcome = await repository.settle(event);

      expect(outcome).toEqual({ kind: "unsold_recorded" });
      expect(await fixtures.orders()).toEqual([]);
      expect(await fixtures.processedEventIds()).toEqual([event.eventId]);
    });
  });

  describe("the same event delivered again", () => {
    it("five times in a row creates one order", async () => {
      const { event } = await fixtures.soldAuction(ownerId, bidderId);

      const outcomes = [];
      for (let delivery = 0; delivery < 5; delivery += 1) {
        outcomes.push(await repository.settle(event));
      }

      expect(outcomes.map(({ kind }) => kind)).toEqual([
        "order_created",
        "duplicate",
        "duplicate",
        "duplicate",
        "duplicate",
      ]);
      expect(await fixtures.orders()).toHaveLength(1);
      expect(await fixtures.processedEventIds()).toEqual([event.eventId]);
    });

    it("at the same moment, from several workers, creates one order", async () => {
      const { event } = await fixtures.soldAuction(ownerId, bidderId);

      const outcomes = await Promise.all(
        Array.from({ length: 10 }, async () => repository.settle(event)),
      );

      const kinds = outcomes.map(({ kind }) => kind);
      expect(kinds.filter((kind) => kind === "order_created")).toHaveLength(1);
      expect(kinds.filter((kind) => kind === "duplicate")).toHaveLength(9);
      expect(await fixtures.orders()).toHaveLength(1);
    });

    it("leaves an unsold auction recorded once", async () => {
      const event = await fixtures.unsoldAuction(ownerId);

      const outcomes = await Promise.all(
        Array.from({ length: 5 }, async () => repository.settle(event)),
      );

      expect(outcomes.filter(({ kind }) => kind === "unsold_recorded")).toHaveLength(1);
      expect(await fixtures.processedEventIds()).toEqual([event.eventId]);
    });
  });

  describe("an event the database refuses", () => {
    async function refusalOf(action: Promise<unknown>): Promise<PermanentSettlementError> {
      const failure = await action.then(
        () => undefined,
        (error: unknown) => error,
      );
      expect(failure).toBeInstanceOf(PermanentSettlementError);
      return failure as PermanentSettlementError;
    }

    it("is a permanent failure for a second, different event of a settled auction", async () => {
      const { event } = await fixtures.soldAuction(ownerId, bidderId);
      await repository.settle(event);
      const second = { ...event, eventId: randomUUID() };

      const refusal = await refusalOf(repository.settle(second));

      expect(refusal).toMatchObject({ sqlState: "23505", constraint: "orders_auction_id_key" });
      // The second event's record was rolled back with its failed order.
      expect(await fixtures.processedEventIds()).toEqual([event.eventId]);
      expect(await fixtures.orders()).toHaveLength(1);
    });

    it("is a permanent failure for an event that names the wrong price, and writes nothing", async () => {
      const { event } = await fixtures.soldAuction(ownerId, bidderId, "1500");
      const tampered = { ...event, finalAmountMinor: "1" };

      const refusal = await refusalOf(repository.settle(tampered));

      expect(refusal).toMatchObject({
        sqlState: "23514",
        constraint: "orders_terms_match_winning_bid",
      });
      expect(await fixtures.processedEventIds()).toEqual([]);
      expect(await fixtures.orders()).toEqual([]);
    });

    it("is a permanent failure for an id too large for the database", async () => {
      const { event } = await fixtures.soldAuction(ownerId, bidderId);

      const refusal = await refusalOf(
        repository.settle({ ...event, auctionId: "99999999999999999999" }),
      );

      expect(refusal.sqlState).toBe("22003");
      expect(await fixtures.processedEventIds()).toEqual([]);
    });

    it("leaves the worker able to settle the next event on the same connection pool", async () => {
      const onlyConnection = new Pool({ connectionString: isolated.url, max: 1 });
      try {
        const single = new PostgresSettlementRepository(wrapPool(onlyConnection));
        const { event } = await fixtures.soldAuction(ownerId, bidderId);
        await refusalOf(single.settle({ ...event, finalAmountMinor: "1" }));

        // The failed connection was destroyed, not left in an aborted
        // transaction; a new one serves the next event.
        const outcome = await single.settle(event);

        expect(outcome.kind).toBe("order_created");
      } finally {
        await onlyConnection.end();
      }
    });
  });

  describe("a failure at the acknowledgement boundary", () => {
    it("is harmless when COMMIT succeeded but its reply was lost", async () => {
      const { event } = await fixtures.soldAuction(ownerId, bidderId);
      const losingReplies = new PostgresSettlementRepository(
        new InterceptingDatabase(database, async (text, client) => {
          if (text === "COMMIT") {
            await client.query("COMMIT");
            throw new Error("connection lost after COMMIT");
          }
          return undefined;
        }),
      );

      // The caller sees a failure, and so will retry the Kafka message...
      await expect(losingReplies.settle(event)).rejects.toThrow("connection lost after COMMIT");
      // ...although the transaction is durable.
      expect(await fixtures.orders()).toHaveLength(1);

      // The retry finds its own earlier work and changes nothing.
      expect(await repository.settle(event)).toEqual({ kind: "duplicate" });
      expect(await fixtures.orders()).toHaveLength(1);
    });

    it("leaves nothing behind when the session dies before COMMIT, and the retry succeeds", async () => {
      const { event } = await fixtures.soldAuction(ownerId, bidderId);
      const dying = new PostgresSettlementRepository(
        new InterceptingDatabase(database, async (text, client) => {
          if (text !== "COMMIT") {
            return undefined;
          }
          const session = await client.query("SELECT pg_backend_pid() AS pid");
          const pid = session.rows[0]!.pid as number;
          await pool.query("SELECT pg_terminate_backend($1)", [pid]);
          await waitUntilGone(pid);
          return undefined;
        }),
      );

      // pg reports a terminated session in one of two words, depending on
      // whether the server's message or the socket closing arrives first.
      await expect(dying.settle(event)).rejects.toThrow(/terminat/i);
      // Neither the order nor the record of the event survived the crash.
      expect(await fixtures.orders()).toEqual([]);
      expect(await fixtures.processedEventIds()).toEqual([]);

      expect(await repository.settle(event)).toEqual({
        kind: "order_created",
        orderId: expect.any(String),
      });
      expect(await fixtures.orders()).toHaveLength(1);
    });

    it("rolls back when a statement fails halfway, so the event is not half recorded", async () => {
      const { event } = await fixtures.soldAuction(ownerId, bidderId);
      const failing = new PostgresSettlementRepository(
        new InterceptingDatabase(database, (text) => {
          if (text.includes("INSERT INTO orders")) {
            throw new Error("connection reset while inserting the order");
          }
          return undefined;
        }),
      );

      await expect(failing.settle(event)).rejects.toThrow("connection reset");

      expect(await fixtures.processedEventIds()).toEqual([]);
      expect(await repository.settle(event)).toMatchObject({ kind: "order_created" });
    });

    async function waitUntilGone(pid: number): Promise<void> {
      const deadline = Date.now() + 10_000;
      for (;;) {
        const result = await pool.query("SELECT 1 FROM pg_stat_activity WHERE pid = $1", [pid]);
        if (result.rows.length === 0) {
          return;
        }
        if (Date.now() > deadline) {
          throw new Error(`backend ${pid} did not exit`);
        }
        await delay(10);
      }
    }
  });

  describe("the check for an event that was never consumed", () => {
    it("lists a sold auction without an order, and only that", async () => {
      const settled = await fixtures.soldAuction(ownerId, bidderId);
      const missed = await fixtures.soldAuction(ownerId, bidderId);
      await fixtures.unsoldAuction(ownerId);
      await repository.settle(settled.event);

      expect(await fixtures.unsettledAuctionIds()).toEqual([missed.event.auctionId]);

      await repository.settle(missed.event);
      expect(await fixtures.unsettledAuctionIds()).toEqual([]);
    });
  });
});
