import { parseAuctionClosedV1 } from "@auction-bonanza/contracts";
import { Pool } from "pg";
import { pino } from "pino";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { PostgresBidRepository } from "../src/bids/bid.repository.js";
import { PostgresFinalizationRepository } from "../src/auctions/finalization.repository.js";
import { FinalizationWorker } from "../src/auctions/finalization.worker.js";
import { wrapPool, type Database } from "../src/database.js";
import { AuctionFixtures } from "./support/outbox-fixtures.js";
import { createIsolatedDatabase, type IsolatedDatabase } from "./support/isolated-database.js";

const adminDatabaseUrl = process.env.TEST_DATABASE_URL;

interface AuctionRow {
  version: string;
  closedAt: Date | null;
  winningBidId: string | null;
  leadingBidId: string | null;
}

// The finalizer claims every due auction in its database, so these tests run
// in a throwaway database that no other test file writes to.
describe.skipIf(adminDatabaseUrl === undefined)("auction finalization with PostgreSQL", () => {
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
    finalization = new PostgresFinalizationRepository(database);
  });

  beforeEach(async () => {
    // Every test starts with nothing due, so a close always concerns its own
    // auctions.
    await pool.query("TRUNCATE outbox_events, bids, auctions, users RESTART IDENTITY CASCADE");
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
      `SELECT version::text, closed_at, winning_bid_id::text, leading_bid_id::text
       FROM auctions WHERE id = $1`,
      [auctionId],
    );
    const row = result.rows[0]!;
    return {
      version: row.version as string,
      closedAt: row.closed_at as Date | null,
      winningBidId: row.winning_bid_id as string | null,
      leadingBidId: row.leading_bid_id as string | null,
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
            repository: new PostgresFinalizationRepository(database),
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

  describe("failure", () => {
    it("leaves the auction untouched when the transaction fails before commit", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      await bid(auctionId, bidderId, "1000");
      await fixtures.expire(auctionId);
      const before = await auctionRow(auctionId);
      const failing = new PostgresFinalizationRepository({
        query: async (text, values) => database.query(text, values),
        connect: async () => {
          const client = await database.connect();
          return {
            query: async (text, values) => {
              if (text.includes("INSERT INTO outbox_events")) {
                throw new Error("connection lost before the event was written");
              }
              return client.query(text, values);
            },
            release: (error) => client.release(error),
          };
        },
      });

      await expect(failing.finalizeNextDue()).rejects.toThrow("connection lost");

      expect(await auctionRow(auctionId)).toEqual(before);
      expect(await closedEvents(auctionId)).toHaveLength(0);

      // The auction is claimed again and closes normally.
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
