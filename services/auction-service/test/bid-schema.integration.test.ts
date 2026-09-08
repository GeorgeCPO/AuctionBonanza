import { randomUUID } from "node:crypto";

import { Pool } from "pg";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

const databaseUrl = process.env.TEST_DATABASE_URL;

interface BidInsert {
  auctionId: string | null;
  bidderId: string | null;
  amountMinor: string | null;
  idempotencyKey: string | null;
  createdAt: string | null;
}

describe.skipIf(databaseUrl === undefined)("bid PostgreSQL schema", () => {
  let database: Pool;
  let ownerId: string;
  let bidderId: string;
  let auctionId: string;

  function validBid(overrides: Partial<BidInsert> = {}): BidInsert {
    return {
      auctionId,
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
      createdAt: "2030-01-01T10:00:00.000000Z",
      ...overrides,
    };
  }

  async function insertBid(overrides: Partial<BidInsert> = {}): Promise<void> {
    const bid = validBid(overrides);
    await database.query(
      `INSERT INTO bids (auction_id, bidder_id, amount_minor, idempotency_key, created_at)
       VALUES ($1, $2, $3, $4, $5)`,
      [bid.auctionId, bid.bidderId, bid.amountMinor, bid.idempotencyKey, bid.createdAt],
    );
  }

  beforeAll(() => {
    database = new Pool({ connectionString: databaseUrl });
  });

  beforeEach(async () => {
    const ownerResult = await database.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'Auction Owner', '$argon2id$test')
       RETURNING id::text`,
      [`bid-schema-owner-${randomUUID()}@example.com`],
    );
    const owner = ownerResult.rows[0];
    if (owner === undefined) throw new Error("owner fixture was not inserted");
    ownerId = owner.id;

    const bidderResult = await database.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'Bidder', '$argon2id$test')
       RETURNING id::text`,
      [`bid-schema-bidder-${randomUUID()}@example.com`],
    );
    const bidder = bidderResult.rows[0];
    if (bidder === undefined) throw new Error("bidder fixture was not inserted");
    bidderId = bidder.id;

    const auctionResult = await database.query<{ id: string }>(
      `INSERT INTO auctions (
         owner_id, title, description, currency_code, starts_at, ends_at,
         starting_price_minor, minimum_bid_increment_minor
       )
       VALUES ($1, 'Vintage clock', 'A mechanical clock.', 'USD',
               now(), now() + interval '1 hour', 1000, 100)
       RETURNING id::text`,
      [ownerId],
    );
    const auction = auctionResult.rows[0];
    if (auction === undefined) throw new Error("auction fixture was not inserted");
    auctionId = auction.id;
  });

  afterEach(async () => {
    await database.query("UPDATE auctions SET leading_bid_id = NULL WHERE owner_id = $1", [
      ownerId,
    ]);
    await database.query("DELETE FROM bids WHERE auction_id = $1", [auctionId]);
    await database.query("DELETE FROM auctions WHERE owner_id = $1", [ownerId]);
    await database.query("DELETE FROM users WHERE id = ANY($1::bigint[])", [[ownerId, bidderId]]);
  });

  afterAll(async () => {
    await database.end();
  });

  it("accepts a well-formed bid", async () => {
    await insertBid();
    const result = await database.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM bids WHERE auction_id = $1",
      [auctionId],
    );
    expect(result.rows[0]?.count).toBe("1");
  });

  it.each([
    ["auction_id", { auctionId: null }],
    ["bidder_id", { bidderId: null }],
    ["amount_minor", { amountMinor: null }],
    ["idempotency_key", { idempotencyKey: null }],
    ["created_at", { createdAt: null }],
  ] satisfies [string, Partial<BidInsert>][])(
    "requires the %s column",
    async (column, overrides) => {
      await expect(insertBid(overrides)).rejects.toMatchObject({
        code: "23502",
        column,
      });
    },
  );

  it("requires an existing auction", async () => {
    await expect(insertBid({ auctionId: "9223372036854775807" })).rejects.toMatchObject({
      code: "23503",
      constraint: "bids_auction_id_fkey",
    });
  });

  it("requires an existing bidder", async () => {
    await expect(insertBid({ bidderId: "9223372036854775807" })).rejects.toMatchObject({
      code: "23503",
      constraint: "bids_bidder_id_fkey",
    });
  });

  it.each(["0", "-1"])("rejects a non-positive amount_minor of %s", async (amountMinor) => {
    await expect(insertBid({ amountMinor })).rejects.toMatchObject({
      code: "23514",
      constraint: "bids_amount_minor_check",
    });
  });

  it.each([
    ["empty", ""],
    ["whitespace-only", "   "],
    ["leading whitespace", " key-1"],
    ["trailing whitespace", "key-1 "],
  ])("rejects an %s idempotency key", async (_case, idempotencyKey) => {
    expect.hasAssertions();
    await expect(insertBid({ idempotencyKey })).rejects.toMatchObject({
      code: "23514",
      constraint: "bids_idempotency_key_check",
    });
  });

  it("limits idempotency keys to 100 characters", async () => {
    await expect(insertBid({ idempotencyKey: "k".repeat(101) })).rejects.toMatchObject({
      code: "22001",
    });
  });

  it("prevents the same bidder from reusing an idempotency key on the same auction", async () => {
    await insertBid({ idempotencyKey: "duplicate-key" });
    await expect(insertBid({ idempotencyKey: "duplicate-key" })).rejects.toMatchObject({
      code: "23505",
      constraint: "bids_auction_bidder_idempotency_key_idx",
    });
  });

  it("allows the same idempotency key across different bidders on the same auction", async () => {
    const otherBidderResult = await database.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'Other Bidder', '$argon2id$test')
       RETURNING id::text`,
      [`bid-schema-other-${randomUUID()}@example.com`],
    );
    const otherBidder = otherBidderResult.rows[0];
    if (otherBidder === undefined) throw new Error("other bidder fixture was not inserted");

    await insertBid({ idempotencyKey: "shared-key" });
    await insertBid({
      bidderId: otherBidder.id,
      idempotencyKey: "shared-key",
      amountMinor: "1100",
    });

    const result = await database.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM bids WHERE auction_id = $1",
      [auctionId],
    );
    expect(result.rows[0]?.count).toBe("2");

    await database.query("DELETE FROM bids WHERE bidder_id = $1", [otherBidder.id]);
    await database.query("DELETE FROM users WHERE id = $1", [otherBidder.id]);
  });

  it("indexes the idempotency lookup and the auction bid-history order", async () => {
    const idempotencyIndex = await database.query<{ indexdef: string }>(
      `SELECT indexdef FROM pg_indexes
       WHERE schemaname = 'public' AND indexname = 'bids_auction_bidder_idempotency_key_idx'`,
    );
    expect(idempotencyIndex.rows[0]?.indexdef).toContain(
      "(auction_id, bidder_id, idempotency_key)",
    );

    const historyIndex = await database.query<{ indexdef: string }>(
      `SELECT indexdef FROM pg_indexes
       WHERE schemaname = 'public' AND indexname = 'bids_auction_id_created_at_id_idx'`,
    );
    expect(historyIndex.rows[0]?.indexdef).toContain("(auction_id, created_at DESC, id DESC)");
  });

  it("links the auction's leading_bid_id to an existing bid and forbids deleting it", async () => {
    const inserted = await database.query<{ id: string }>(
      `INSERT INTO bids (auction_id, bidder_id, amount_minor, idempotency_key, created_at)
       VALUES ($1, $2, 1000, 'key-1', now())
       RETURNING id::text`,
      [auctionId, bidderId],
    );
    const bidId = inserted.rows[0]?.id;
    await database.query("UPDATE auctions SET leading_bid_id = $1 WHERE id = $2", [
      bidId,
      auctionId,
    ]);

    await expect(database.query("DELETE FROM bids WHERE id = $1", [bidId])).rejects.toMatchObject({
      code: "23503",
      constraint: "auctions_leading_bid_id_fkey",
    });

    await expect(
      database.query("UPDATE auctions SET leading_bid_id = $1 WHERE id = $2", [
        "9223372036854775807",
        auctionId,
      ]),
    ).rejects.toMatchObject({ code: "23503", constraint: "auctions_leading_bid_id_fkey" });
  });
});
