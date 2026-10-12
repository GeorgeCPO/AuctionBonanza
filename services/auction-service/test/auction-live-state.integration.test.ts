import { randomUUID } from "node:crypto";

import { Pool } from "pg";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import type { AuctionLiveState } from "../src/auctions/auction.js";
import { PostgresAuctionRepository } from "../src/auctions/auction.repository.js";
import { PostgresBidRepository } from "../src/bids/bid.repository.js";
import type { Database } from "../src/database.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(databaseUrl === undefined)("auction live-state PostgreSQL read", () => {
  let pool: Pool;
  let auctions: PostgresAuctionRepository;
  let bids: PostgresBidRepository;
  let ownerId: string;
  let bidderIds: string[];

  beforeAll(() => {
    pool = new Pool({ connectionString: databaseUrl });
    const database: Database = {
      query: async (text, values) => pool.query(text, values as unknown[] | undefined),
      connect: async () => {
        const client = await pool.connect();
        return {
          query: async (text, values) => client.query(text, values as unknown[] | undefined),
          release: () => client.release(),
        };
      },
    };
    auctions = new PostgresAuctionRepository(database);
    bids = new PostgresBidRepository(database);
  });

  async function insertUser(label: string): Promise<string> {
    const result = await pool.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, $2, '$argon2id$test')
       RETURNING id::text`,
      [`live-state-${label}-${randomUUID()}@example.com`, `Live ${label}`],
    );
    return result.rows[0]!.id;
  }

  async function insertOpenAuction(options: { cancelled?: boolean } = {}): Promise<string> {
    const result = await pool.query<{ id: string }>(
      `INSERT INTO auctions (
         owner_id, title, description, currency_code, starts_at, ends_at,
         starting_price_minor, minimum_bid_increment_minor, cancelled_at
       )
       VALUES ($1, 'Live lot', 'A live-state fixture.', 'USD',
               clock_timestamp() - interval '1 hour', clock_timestamp() + interval '1 hour',
               1000, 100, CASE WHEN $2 THEN clock_timestamp() ELSE NULL END)
       RETURNING id::text`,
      [ownerId, options.cancelled === true],
    );
    return result.rows[0]!.id;
  }

  async function placeBid(auctionId: string, bidderId: string, amountMinor: string) {
    const outcome = await bids.placeBid({
      auctionId,
      bidderId,
      amountMinor,
      idempotencyKey: randomUUID(),
    });
    if (outcome.kind !== "placed") {
      throw new Error(`Fixture bid was not placed: ${outcome.kind}`);
    }
    return outcome;
  }

  beforeEach(async () => {
    ownerId = await insertUser("owner");
    bidderIds = [await insertUser("bidder-a"), await insertUser("bidder-b")];
  });

  afterEach(async () => {
    const auctionIds = `SELECT id FROM auctions WHERE owner_id = $1`;
    await pool.query(`UPDATE auctions SET leading_bid_id = NULL WHERE owner_id = $1`, [ownerId]);
    await pool.query(`DELETE FROM outbox_events WHERE auction_id IN (${auctionIds})`, [ownerId]);
    await pool.query(`DELETE FROM bids WHERE auction_id IN (${auctionIds})`, [ownerId]);
    await pool.query(`DELETE FROM auctions WHERE owner_id = $1`, [ownerId]);
    await pool.query(`DELETE FROM users WHERE id = ANY($1::bigint[])`, [[ownerId, ...bidderIds]]);
  });

  afterAll(async () => {
    await pool.end();
  });

  it("returns the version, leading bid, and derived phase of each existing auction", async () => {
    const withBids = await insertOpenAuction();
    const withoutBids = await insertOpenAuction();
    const cancelled = await insertOpenAuction({ cancelled: true });
    await placeBid(withBids, bidderIds[0]!, "1000");
    const leading = await placeBid(withBids, bidderIds[1]!, "1100");

    const snapshot = await auctions.findLiveStates([
      withBids,
      "9223372036854775807",
      withoutBids,
      cancelled,
    ]);

    const byId = new Map(snapshot.states.map((state) => [state.auctionId, state]));
    expect(byId.size).toBe(3);
    expect(byId.get(withBids)).toMatchObject({
      auctionVersion: "3",
      currencyCode: "USD",
      phase: "open",
      cancelledAt: null,
      closedAt: null,
      leadingBid: { bidId: leading.bid.id, amountMinor: "1100" },
    });
    expect(byId.get(withoutBids)).toMatchObject({
      auctionVersion: "1",
      phase: "open",
      leadingBid: null,
    });
    expect(byId.get(cancelled)).toMatchObject({ phase: "cancelled", leadingBid: null });
    expect(byId.get(cancelled)!.cancelledAt).toBeInstanceOf(Date);
    expect(snapshot.observedAt).toBeInstanceOf(Date);
  });

  it("returns the observed instant when no requested auction exists", async () => {
    const snapshot = await auctions.findLiveStates(["9223372036854775807"]);

    expect(snapshot.states).toEqual([]);
    expect(snapshot.observedAt).toBeInstanceOf(Date);
  });

  it("never pairs a version with another version's leading bid under concurrent bidding", async () => {
    const auctionId = await insertOpenAuction();
    const bidCount = 30;
    let bidding = true;

    const bidder = (async () => {
      try {
        for (let index = 0; index < bidCount; index += 1) {
          await placeBid(auctionId, bidderIds[index % 2]!, String(1000 + index * 100));
        }
      } finally {
        bidding = false;
      }
    })();
    const reads: AuctionLiveState[] = [];
    const reader = (async () => {
      while (bidding) {
        const snapshot = await auctions.findLiveStates([auctionId]);
        reads.push(snapshot.states[0]!);
      }
    })();
    await Promise.all([bidder, reader]);
    reads.push((await auctions.findLiveStates([auctionId])).states[0]!);

    // The event that produced each version names the bid it made leading.
    const events = await pool.query<{ version: string; bid_id: string; amount_minor: string }>(
      `SELECT auction_version::text AS version,
              payload->>'bidId' AS bid_id,
              payload->>'amountMinor' AS amount_minor
       FROM outbox_events
       WHERE auction_id = $1`,
      [auctionId],
    );
    const leadingBidByVersion = new Map(
      events.rows.map((row) => [row.version, { bidId: row.bid_id, amountMinor: row.amount_minor }]),
    );
    expect(leadingBidByVersion.size).toBe(bidCount);

    const versionsSeen = new Set<string>();
    for (const state of reads) {
      versionsSeen.add(state.auctionVersion);
      const expected =
        state.auctionVersion === "1" ? null : leadingBidByVersion.get(state.auctionVersion);
      expect(expected).toBeDefined();
      expect(state.leadingBid).toEqual(expected);
    }
    // The reader must actually have raced the writer, or the test proves nothing.
    expect(versionsSeen.size).toBeGreaterThan(2);
    expect(reads.at(-1)!.auctionVersion).toBe(String(bidCount + 1));
  });
});
