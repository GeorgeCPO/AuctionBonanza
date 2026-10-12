import { parseAuctionClosedV1 } from "@auction-bonanza/contracts";
import { Pool } from "pg";
import { pino } from "pino";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { PostgresBidRepository } from "../src/bids/bid.repository.js";
import { PostgresFinalizationRepository } from "../src/auctions/finalization.repository.js";
import { FinalizationWorker } from "../src/auctions/finalization.worker.js";
import { wrapPool, type Database, type QueryResult } from "../src/database.js";
import { AuctionFixtures } from "./support/outbox-fixtures.js";
import { createIsolatedDatabase, type IsolatedDatabase } from "./support/isolated-database.js";

const adminDatabaseUrl = process.env.TEST_DATABASE_URL;

interface AuctionRow {
  version: string;
  updatedAt: Date;
  closedAt: Date | null;
  winningBidId: string | null;
  leadingBidId: string | null;
  attempts: number;
  nextAttemptAt: Date | null;
  lastError: string | null;
}

// The finalizer claims every due auction in its database, so these tests run
// in a throwaway database that no other test file writes to.
describe.skipIf(adminDatabaseUrl === undefined)("auction finalization with PostgreSQL", () => {
  // Every failed close is rescheduled 30 s ahead (equal jitter with the random
  // draw pinned to 0), far longer than a test runs. Tests make a retry due
  // explicitly, which keeps them deterministic.
  const RETRY_DELAY_MS = 30_000;

  function repositoryFor(db: Database): PostgresFinalizationRepository {
    return new PostgresFinalizationRepository(db, {
      retryBackoff: { baseDelayMs: 60_000, maxDelayMs: 60_000 },
      random: () => 0,
    });
  }

  let isolated: IsolatedDatabase;
  let pool: Pool;
  let database: Database;
  let fixtures: AuctionFixtures;
  let bids: PostgresBidRepository;
  let finalization: PostgresFinalizationRepository;
  let ownerId: string;
  let bidderId: string;
  let otherBidderId: string;

  beforeAll(async () => {
    isolated = await createIsolatedDatabase(adminDatabaseUrl!);
    pool = new Pool({ connectionString: isolated.url, max: 8 });
    database = wrapPool(pool);
    fixtures = new AuctionFixtures(pool);
    bids = new PostgresBidRepository(database);
    finalization = repositoryFor(database);
    // A way to make closing chosen auctions fail for real: the database
    // rejects the close, exactly as a constraint or trigger would. Removing
    // the auction from the table removes the cause.
    await pool.query(`
      CREATE TABLE test_poisoned_auctions (auction_id bigint PRIMARY KEY);
      CREATE FUNCTION test_reject_poisoned_close() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        IF OLD.closed_at IS NULL AND NEW.closed_at IS NOT NULL
          AND EXISTS (SELECT 1 FROM test_poisoned_auctions WHERE auction_id = NEW.id) THEN
          RAISE EXCEPTION 'poisoned auction %', NEW.id USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
      END;
      $$;
      CREATE TRIGGER test_poisoned_close BEFORE UPDATE ON auctions
        FOR EACH ROW EXECUTE FUNCTION test_reject_poisoned_close();
    `);
  });

  beforeEach(async () => {
    // Every test starts with nothing due, so a close always concerns its own
    // auctions.
    await pool.query(
      "TRUNCATE test_poisoned_auctions, outbox_events, bids, auctions, users RESTART IDENTITY CASCADE",
    );
    ownerId = await fixtures.user();
    bidderId = await fixtures.user();
    otherBidderId = await fixtures.user();
  });

  afterAll(async () => {
    await pool.end();
    await isolated.drop();
  });

  async function bid(auctionId: string, bidder: string, amountMinor: string): Promise<string> {
    const outcome = await bids.placeBid({
      auctionId,
      bidderId: bidder,
      amountMinor,
      idempotencyKey: `${bidder}-${amountMinor}`,
    });
    if (outcome.kind !== "placed") {
      throw new Error(`fixture bid was not placed: ${outcome.kind}`);
    }
    return outcome.bid.id;
  }

  async function auctionRow(auctionId: string): Promise<AuctionRow> {
    const result = await pool.query<Record<string, unknown>>(
      `SELECT version::text, updated_at, closed_at, winning_bid_id::text, leading_bid_id::text,
              finalization_attempts, next_finalization_attempt_at, last_finalization_error
       FROM auctions WHERE id = $1`,
      [auctionId],
    );
    const row = result.rows[0]!;
    return {
      version: row.version as string,
      updatedAt: row.updated_at as Date,
      closedAt: row.closed_at as Date | null,
      winningBidId: row.winning_bid_id as string | null,
      leadingBidId: row.leading_bid_id as string | null,
      attempts: row.finalization_attempts as number,
      nextAttemptAt: row.next_finalization_attempt_at as Date | null,
      lastError: row.last_finalization_error as string | null,
    };
  }

  // A database that behaves like `database` except that `intercept` may
  // answer a query on a checked-out client itself (return undefined to let it
  // through). `onRelease` sees how each client is handed back.
  function interceptingDatabase(
    intercept: (text: string) => Promise<QueryResult | undefined> | undefined,
    onRelease?: (error?: Error) => void,
  ): Database {
    return {
      query: async (text, values) => database.query(text, values),
      connect: async () => {
        const client = await database.connect();
        return {
          query: async (text, values) => (await intercept(text)) ?? client.query(text, values),
          release: (error) => {
            onRelease?.(error);
            client.release(error);
          },
        };
      },
    };
  }

  async function closedEvents(auctionId: string): Promise<{ version: string; payload: unknown }[]> {
    const result = await pool.query<{ version: string; payload: unknown }>(
      `SELECT auction_version::text AS version, payload
       FROM outbox_events
       WHERE auction_id = $1 AND event_type = 'AuctionClosed.v1'`,
      [auctionId],
    );
    return result.rows;
  }

  describe("closing", () => {
    it("persists the leading bid as the winner with one version step and one event", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await bid(auctionId, bidderId, "1000");
      const winningBidId = await bid(auctionId, otherBidderId, "1100");
      await fixtures.expire(auctionId);
      const before = await auctionRow(auctionId);

      const outcome = await finalization.finalizeNextDue();

      expect(outcome).toEqual({
        kind: "closed",
        auctionId,
        auctionVersion: String(BigInt(before.version) + 1n),
        winningBidId,
      });
      const after = await auctionRow(auctionId);
      expect(after.closedAt).toBeInstanceOf(Date);
      expect(after).toMatchObject({
        version: String(BigInt(before.version) + 1n),
        winningBidId,
        leadingBidId: winningBidId,
      });
      const events = await closedEvents(auctionId);
      expect(events).toHaveLength(1);
      expect(events[0]!.version).toBe(after.version);
      expect(parseAuctionClosedV1(events[0]!.payload)).toMatchObject({
        auctionId,
        auctionVersion: after.version,
        currencyCode: "EUR",
        winningBidId,
        winnerId: otherBidderId,
        finalAmountMinor: "1100",
      });
    });

    it("closes an auction without bids as unsold", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await fixtures.expire(auctionId);
      const before = await auctionRow(auctionId);

      const outcome = await finalization.finalizeNextDue();

      expect(outcome).toEqual({
        kind: "closed",
        auctionId,
        auctionVersion: String(BigInt(before.version) + 1n),
        winningBidId: null,
      });
      const after = await auctionRow(auctionId);
      expect(after.closedAt).toBeInstanceOf(Date);
      expect(after.winningBidId).toBeNull();
      const events = await closedEvents(auctionId);
      expect(parseAuctionClosedV1(events[0]!.payload)).toMatchObject({
        winningBidId: null,
        winnerId: null,
        finalAmountMinor: null,
      });
    });

    it("closes at or after the deadline, never before it", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await bid(auctionId, bidderId, "1000");
      await fixtures.expire(auctionId);

      await finalization.finalizeNextDue();

      const result = await pool.query<{ after_deadline: boolean }>(
        "SELECT closed_at >= ends_at AS after_deadline FROM auctions WHERE id = $1",
        [auctionId],
      );
      expect(result.rows[0]!.after_deadline).toBe(true);
    });
  });

  describe("leaving auctions alone", () => {
    it("does nothing when no auction is due", async () => {
      const openId = await fixtures.openAuction(ownerId);
      await bid(openId, bidderId, "1000");
      const before = await auctionRow(openId);

      await expect(finalization.finalizeNextDue()).resolves.toEqual({ kind: "none_due" });

      expect(await auctionRow(openId)).toEqual(before);
      expect(await closedEvents(openId)).toHaveLength(0);
    });

    it("does not close a cancelled auction", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await fixtures.expire(auctionId);
      // Written directly, without a version step, because only the
      // cancelled state matters here.
      await pool.query(
        "UPDATE auctions SET cancelled_at = clock_timestamp() - interval '1 hour' WHERE id = $1",
        [auctionId],
      );

      await expect(finalization.finalizeNextDue()).resolves.toEqual({ kind: "none_due" });

      expect((await auctionRow(auctionId)).closedAt).toBeNull();
      expect(await closedEvents(auctionId)).toHaveLength(0);
    });

    it("closes an auction once: a second run changes nothing and emits nothing", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await bid(auctionId, bidderId, "1000");
      await fixtures.expire(auctionId);
      await finalization.finalizeNextDue();
      const closed = await auctionRow(auctionId);

      await expect(finalization.finalizeNextDue()).resolves.toEqual({ kind: "none_due" });

      expect(await auctionRow(auctionId)).toEqual(closed);
      expect(await closedEvents(auctionId)).toHaveLength(1);
    });

    it("rejects bids after the close", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await bid(auctionId, bidderId, "1000");
      await fixtures.expire(auctionId);
      await finalization.finalizeNextDue();

      const late = await bids.placeBid({
        auctionId,
        bidderId: otherBidderId,
        amountMinor: "5000",
        idempotencyKey: "late",
      });

      expect(late).toEqual({ kind: "auction_not_open", phase: "closed" });
    });
  });

  describe("concurrency", () => {
    it("lets competing finalizers close each auction exactly once", async () => {
      const auctionIds: string[] = [];
      for (let index = 0; index < 12; index += 1) {
        const auctionId = await fixtures.openAuction(ownerId);
        if (index % 3 !== 0) {
          await bid(auctionId, bidderId, "1000");
        }
        await fixtures.expire(auctionId);
        auctionIds.push(auctionId);
      }

      const closedCounts = await Promise.all(
        [1, 2, 3, 4].map(async () => {
          const worker = new FinalizationWorker({
            repository: repositoryFor(database),
            pollIntervalMs: 10,
            retryBackoff: { baseDelayMs: 10, maxDelayMs: 10 },
            logger: pino({ level: "silent" }),
          });
          return worker.drain();
        }),
      );

      expect(closedCounts.reduce((total, count) => total + count, 0)).toBe(12);
      const sold = await pool.query<{ count: string }>(
        "SELECT count(*) FROM auctions WHERE winning_bid_id IS NOT NULL",
      );
      expect(sold.rows[0]!.count).toBe("8");
      for (const auctionId of auctionIds) {
        const events = await closedEvents(auctionId);
        expect(events).toHaveLength(1);
        expect((await auctionRow(auctionId)).version).toBe(events[0]!.version);
      }
    });

    it("passes over an auction locked by an in-flight bid instead of waiting for it", async () => {
      const lockedId = await fixtures.openAuction(ownerId);
      await bid(lockedId, bidderId, "1000");
      await fixtures.expire(lockedId);

      const locker = await pool.connect();
      try {
        await locker.query("BEGIN");
        await locker.query("SELECT id FROM auctions WHERE id = $1 FOR UPDATE", [lockedId]);

        await expect(finalization.finalizeNextDue()).resolves.toEqual({ kind: "none_due" });
        expect((await auctionRow(lockedId)).closedAt).toBeNull();

        await locker.query("COMMIT");
      } finally {
        locker.release();
      }

      const outcome = await finalization.finalizeNextDue();
      expect(outcome.kind).toBe("closed");
    });

    it("names the highest bid accepted before the deadline as the winner", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await bid(auctionId, bidderId, "1000");
      await bid(auctionId, otherBidderId, "1100");
      const highest = await bid(auctionId, bidderId, "1200");
      await fixtures.expire(auctionId);

      // A bid racing the close is rejected: the deadline has passed. If it
      // held the row lock, the finalizer passes over the auction and closes
      // it on its next poll. Neither ordering may name a lower bid.
      const [late] = await Promise.all([
        bids.placeBid({
          auctionId,
          bidderId: otherBidderId,
          amountMinor: "1300",
          idempotencyKey: "racing",
        }),
        finalization.finalizeNextDue(),
      ]);

      await finalization.finalizeNextDue();

      expect(late.kind).toBe("auction_not_open");
      expect((await auctionRow(auctionId)).winningBidId).toBe(highest);
      expect(await closedEvents(auctionId)).toHaveLength(1);
    });
  });

  describe("a close that fails", () => {
    async function poison(auctionId: string): Promise<void> {
      await pool.query("INSERT INTO test_poisoned_auctions (auction_id) VALUES ($1)", [auctionId]);
    }

    async function unpoison(auctionId: string): Promise<void> {
      await pool.query("DELETE FROM test_poisoned_auctions WHERE auction_id = $1", [auctionId]);
    }

    async function retryIsScheduledWithinAMinute(auctionId: string): Promise<boolean> {
      const result = await pool.query<{ scheduled: boolean }>(
        `SELECT next_finalization_attempt_at > clock_timestamp()
                AND next_finalization_attempt_at < clock_timestamp() + interval '1 minute'
                AS scheduled
         FROM auctions WHERE id = $1`,
        [auctionId],
      );
      return result.rows[0]!.scheduled;
    }

    // An expired auction with a bid, so the close has a winner to name.
    async function expiredAuction(): Promise<string> {
      const auctionId = await fixtures.openAuction(ownerId);
      await bid(auctionId, bidderId, "1000");
      await fixtures.expire(auctionId);
      return auctionId;
    }

    it("does not block the auctions due after it", async () => {
      const poisoned = await expiredAuction();
      const healthy = await expiredAuction();
      await poison(poisoned);

      const first = await finalization.finalizeNextDue();
      const second = await finalization.finalizeNextDue();
      const third = await finalization.finalizeNextDue();

      expect(first).toMatchObject({
        kind: "close_failed",
        auctionId: poisoned,
        attempt: 1,
        retryDelayMs: RETRY_DELAY_MS,
      });
      expect(second).toMatchObject({ kind: "closed", auctionId: healthy });
      expect(third).toEqual({ kind: "none_due" });
    });

    it("records the failure although the close rolled back, and changes nothing else", async () => {
      const poisoned = await expiredAuction();
      await poison(poisoned);
      const before = await auctionRow(poisoned);

      await finalization.finalizeNextDue();

      const after = await auctionRow(poisoned);
      expect(after).toMatchObject({
        version: before.version,
        updatedAt: before.updatedAt,
        closedAt: null,
        winningBidId: null,
        attempts: 1,
      });
      expect(after.lastError).toContain("poisoned auction");
      expect(await retryIsScheduledWithinAMinute(poisoned)).toBe(true);
      expect(await closedEvents(poisoned)).toHaveLength(0);
    });

    it("is retried only after its backoff, and closes once the cause is gone", async () => {
      const poisoned = await expiredAuction();
      await poison(poisoned);
      await finalization.finalizeNextDue();
      await unpoison(poisoned);

      await expect(finalization.finalizeNextDue()).resolves.toEqual({ kind: "none_due" });

      await fixtures.makeFinalizationRetriesDue();
      await expect(finalization.finalizeNextDue()).resolves.toMatchObject({
        kind: "closed",
        auctionId: poisoned,
      });
      const after = await auctionRow(poisoned);
      expect(after.closedAt).toBeInstanceOf(Date);
      expect(after.attempts).toBe(1);
      expect(after.lastError).toContain("poisoned auction");
      expect(await closedEvents(poisoned)).toHaveLength(1);
    });

    it("counts attempts and backs off further after each failure", async () => {
      const poisoned = await expiredAuction();
      await poison(poisoned);

      await finalization.finalizeNextDue();
      await fixtures.makeFinalizationRetriesDue();
      const second = await finalization.finalizeNextDue();

      expect(second).toMatchObject({ kind: "close_failed", attempt: 2 });
      expect((await auctionRow(poisoned)).attempts).toBe(2);
    });

    it("keeps the error text within the column width and free of NUL characters", async () => {
      const auctionId = await expiredAuction();
      const failing = repositoryFor(
        interceptingDatabase((text) => {
          if (text.includes("INSERT INTO outbox_events")) {
            throw new Error(`${"x".repeat(2_000)}\u0000tail`);
          }
          return undefined;
        }),
      );

      await expect(failing.finalizeNextDue()).resolves.toMatchObject({
        kind: "close_failed",
        auctionId,
      });

      const { lastError } = await auctionRow(auctionId);
      expect(lastError).toHaveLength(1_000);
      expect(lastError).not.toContain("\u0000");
    });

    it("records a missing outbox event as a failed close instead of failing the commit", async () => {
      const auctionId = await expiredAuction();
      const before = await auctionRow(auctionId);
      // The event insert is swallowed, so the deferred "version has an event"
      // check would fail at COMMIT. Making it immediate inside the savepoint
      // lets the failure be recorded instead.
      const skippingEvent = repositoryFor(
        interceptingDatabase((text) =>
          text.includes("INSERT INTO outbox_events")
            ? Promise.resolve({ rows: [], rowCount: 0 })
            : undefined,
        ),
      );

      const outcome = await skippingEvent.finalizeNextDue();

      expect(outcome).toMatchObject({ kind: "close_failed", auctionId, attempt: 1 });
      expect(outcome.kind === "close_failed" && outcome.error.message).toContain(
        "has no outbox event",
      );
      expect(await auctionRow(auctionId)).toMatchObject({
        version: before.version,
        closedAt: null,
        attempts: 1,
      });
    });

    it("keeps the row locked while the failure is recorded", async () => {
      const poisoned = await expiredAuction();
      await poison(poisoned);
      let competingLock: unknown;
      const observing = repositoryFor(
        interceptingDatabase(async (text) => {
          if (text.includes("last_finalization_error =")) {
            // The close has been rolled back to the savepoint. If the claim's
            // lock were gone, another session could take it now.
            competingLock = await pool
              .query("SELECT id FROM auctions WHERE id = $1 FOR UPDATE NOWAIT", [poisoned])
              .catch((error: unknown) => error);
          }
          return undefined;
        }),
      );

      await observing.finalizeNextDue();

      expect(competingLock).toMatchObject({ code: "55P03" });
    });

    it("rejects, and records nothing, when the failure cannot be recorded", async () => {
      const auctionId = await expiredAuction();
      const before = await auctionRow(auctionId);
      const released: unknown[] = [];
      const brokenConnection = repositoryFor(
        interceptingDatabase(
          (text) => {
            if (text.includes("INSERT INTO outbox_events")) {
              throw new Error("connection lost before the event was written");
            }
            if (text.includes("ROLLBACK TO SAVEPOINT")) {
              throw new Error("connection lost");
            }
            return undefined;
          },
          (error) => released.push(error),
        ),
      );

      await expect(brokenConnection.finalizeNextDue()).rejects.toThrow(
        /Recording the failed close of auction \d+ failed/,
      );

      expect(released).toHaveLength(1);
      expect(released[0]).toBeInstanceOf(Error);
      expect(await auctionRow(auctionId)).toEqual(before);
      // The auction is simply claimed again by the next call.
      await expect(finalization.finalizeNextDue()).resolves.toMatchObject({ kind: "closed" });
    });

    it("schedules the retry on the database clock", async () => {
      const poisoned = await expiredAuction();
      await poison(poisoned);
      vi.useFakeTimers({ toFake: ["Date"] });
      vi.setSystemTime(new Date("2000-01-01T00:00:00Z"));
      try {
        await finalization.finalizeNextDue();
      } finally {
        vi.useRealTimers();
      }

      expect(await retryIsScheduledWithinAMinute(poisoned)).toBe(true);
    });

    it("leaves every healthy auction closed exactly once when finalizers compete", async () => {
      const poisoned = await expiredAuction();
      await poison(poisoned);
      const healthy: string[] = [];
      for (let index = 0; index < 12; index += 1) {
        healthy.push(await expiredAuction());
      }

      const closedCounts = await Promise.all(
        [1, 2, 3].map(async () =>
          new FinalizationWorker({
            repository: repositoryFor(database),
            pollIntervalMs: 10,
            retryBackoff: { baseDelayMs: 10, maxDelayMs: 10 },
            logger: pino({ level: "silent" }),
          }).drain(),
        ),
      );

      expect(closedCounts.reduce((total, count) => total + count, 0)).toBe(12);
      for (const auctionId of healthy) {
        const events = await closedEvents(auctionId);
        expect(events).toHaveLength(1);
        expect((await auctionRow(auctionId)).version).toBe(events[0]!.version);
      }
      expect(await auctionRow(poisoned)).toMatchObject({ closedAt: null, attempts: 1 });
      expect(await closedEvents(poisoned)).toHaveLength(0);
    });

    it("still closes healthy auctions in deadline order around a poisoned one", async () => {
      const first = await expiredAuction();
      const poisoned = await expiredAuction();
      const third = await expiredAuction();
      const fourth = await expiredAuction();
      await poison(poisoned);

      const order: string[] = [];
      for (;;) {
        const outcome = await finalization.finalizeNextDue();
        if (outcome.kind === "none_due") {
          break;
        }
        order.push(outcome.auctionId);
      }

      expect(order).toEqual([first, poisoned, third, fourth]);
    });

    it("claims auctions with the same deadline in numeric id order", async () => {
      const ids: string[] = [];
      for (let index = 0; index < 11; index += 1) {
        ids.push(await fixtures.openAuction(ownerId));
      }
      await pool.query(
        `UPDATE auctions
         SET starts_at = TIMESTAMPTZ '2000-01-01 00:00:00+00',
             ends_at = TIMESTAMPTZ '2000-01-01 01:00:00+00'
         WHERE id = ANY($1::bigint[])`,
        [ids],
      );

      const order: string[] = [];
      for (;;) {
        const outcome = await finalization.finalizeNextDue();
        if (outcome.kind === "none_due") {
          break;
        }
        order.push(outcome.auctionId);
      }

      // Text order would put id 10 and 11 before id 2.
      expect(order).toEqual(ids);
    });
  });

  describe("failure", () => {
    it("records a failure of the transaction itself and leaves the auction closable", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await bid(auctionId, bidderId, "1000");
      await fixtures.expire(auctionId);
      const failing = repositoryFor(
        interceptingDatabase((text) => {
          if (text.includes("INSERT INTO outbox_events")) {
            throw new Error("event could not be written");
          }
          return undefined;
        }),
      );

      await expect(failing.finalizeNextDue()).resolves.toMatchObject({
        kind: "close_failed",
        auctionId,
      });

      expect((await auctionRow(auctionId)).closedAt).toBeNull();
      expect(await closedEvents(auctionId)).toHaveLength(0);
      await fixtures.makeFinalizationRetriesDue();
      await expect(finalization.finalizeNextDue()).resolves.toMatchObject({ kind: "closed" });
      expect(await closedEvents(auctionId)).toHaveLength(1);
    });
  });

  describe("schema invariants", () => {
    async function closeWith(auctionId: string, assignments: string): Promise<void> {
      await pool.query(`UPDATE auctions SET ${assignments}, version = version + 1 WHERE id = $1`, [
        auctionId,
      ]);
    }

    it("rejects a winner on an auction that is not closed", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      const bidId = await bid(auctionId, bidderId, "1000");

      await expect(
        pool.query("UPDATE auctions SET winning_bid_id = $1 WHERE id = $2", [bidId, auctionId]),
      ).rejects.toMatchObject({ constraint: "auctions_winning_bid_closed_check" });
    });

    // The close trigger rejects this first. The composite foreign key
    // (auctions_winning_bid_id_fkey) is a second, independent layer.
    it("rejects a winner that is another auction's bid", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      const otherAuctionId = await fixtures.openAuction(ownerId);
      const otherBidId = await bid(otherAuctionId, bidderId, "1000");
      await fixtures.expire(auctionId);

      await expect(
        pool.query(
          `UPDATE auctions
           SET closed_at = clock_timestamp(), winning_bid_id = $1, version = version + 1
           WHERE id = $2`,
          [otherBidId, auctionId],
        ),
      ).rejects.toMatchObject({ constraint: "auctions_winner_is_leading_bid" });
    });

    it("rejects closing with a winner other than the leading bid", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      const first = await bid(auctionId, bidderId, "1000");
      await bid(auctionId, otherBidderId, "1100");
      await fixtures.expire(auctionId);

      await expect(
        closeWith(auctionId, `closed_at = clock_timestamp(), winning_bid_id = ${first}`),
      ).rejects.toMatchObject({ constraint: "auctions_winner_is_leading_bid" });
    });

    it("rejects closing a bid-bearing auction as unsold", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await bid(auctionId, bidderId, "1000");
      await fixtures.expire(auctionId);

      await expect(closeWith(auctionId, "closed_at = clock_timestamp()")).rejects.toMatchObject({
        constraint: "auctions_winner_is_leading_bid",
      });
    });

    it("rejects closing without the version step and event", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await fixtures.expire(auctionId);

      // The version trigger demands an event for every version change, so a
      // close that bumps the version without one cannot commit.
      await expect(closeWith(auctionId, "closed_at = clock_timestamp()")).rejects.toMatchObject({
        constraint: "auctions_version_outbox_event",
      });
    });

    it("keeps a closed auction's outcome immutable", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await bid(auctionId, bidderId, "1000");
      await fixtures.expire(auctionId);
      await finalization.finalizeNextDue();

      await expect(
        pool.query("UPDATE auctions SET winning_bid_id = NULL WHERE id = $1", [auctionId]),
      ).rejects.toMatchObject({ constraint: "auctions_closed_outcome_immutable" });
      await expect(
        pool.query(
          "UPDATE auctions SET closed_at = closed_at + interval '1 second' WHERE id = $1",
          [auctionId],
        ),
      ).rejects.toMatchObject({ constraint: "auctions_closed_outcome_immutable" });
    });
  });
});
