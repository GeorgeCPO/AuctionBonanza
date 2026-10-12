import { randomUUID } from "node:crypto";

import { Pool } from "pg";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { PostgresBidRepository } from "../src/bids/bid.repository.js";
import type { Database } from "../src/database.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

interface AuctionFixtureOptions {
  startsAtOffsetMs?: number;
  endsAtOffsetMs?: number;
  startingPriceMinor?: string;
  minimumBidIncrementMinor?: string;
  cancelled?: boolean;
  closed?: boolean;
}

describe.skipIf(databaseUrl === undefined)("bid PostgreSQL repository", () => {
  let pool: Pool;
  let database: Database;
  let repository: PostgresBidRepository;
  let ownerId: string;
  let bidderId: string;
  let otherBidderId: string;

  beforeAll(() => {
    pool = new Pool({ connectionString: databaseUrl });
    database = {
      query: async (text, values) => pool.query(text, values as unknown[] | undefined),
      connect: async () => {
        const client = await pool.connect();
        return {
          query: async (text, values) => client.query(text, values as unknown[] | undefined),
          release: () => client.release(),
        };
      },
    };
    repository = new PostgresBidRepository(database);
  });

  async function insertUser(label: string): Promise<string> {
    const result = await pool.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, $2, '$argon2id$test')
       RETURNING id::text`,
      [`bid-repo-${label}-${randomUUID()}@example.com`, `Bid ${label}`],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`${label} fixture was not inserted`);
    }
    return row.id;
  }

  beforeEach(async () => {
    ownerId = await insertUser("owner");
    bidderId = await insertUser("bidder");
    otherBidderId = await insertUser("other-bidder");
  });

  afterEach(async () => {
    // auctions.leading_bid_id -> bids and bids.auction_id -> auctions point at
    // each other, so the leading-bid reference must be cleared before either
    // table's rows for this fixture can be deleted.
    await pool.query(
      `UPDATE auctions SET leading_bid_id = NULL WHERE owner_id = ANY($1::bigint[])`,
      [[ownerId, bidderId, otherBidderId]],
    );
    await pool.query(
      `DELETE FROM outbox_events WHERE auction_id IN (
         SELECT id FROM auctions WHERE owner_id = ANY($1::bigint[])
       )`,
      [[ownerId, bidderId, otherBidderId]],
    );
    await pool.query(
      `DELETE FROM bids WHERE auction_id IN (
         SELECT id FROM auctions WHERE owner_id = ANY($1::bigint[])
       )`,
      [[ownerId, bidderId, otherBidderId]],
    );
    await pool.query(`DELETE FROM auctions WHERE owner_id = ANY($1::bigint[])`, [
      [ownerId, bidderId, otherBidderId],
    ]);
    await pool.query(`DELETE FROM users WHERE id = ANY($1::bigint[])`, [
      [ownerId, bidderId, otherBidderId],
    ]);
  });

  afterAll(async () => {
    await pool.end();
  });

  async function insertAuction(options: AuctionFixtureOptions = {}): Promise<string> {
    const startsAtOffsetMs = options.startsAtOffsetMs ?? -60 * 60_000;
    const endsAtOffsetMs = options.endsAtOffsetMs ?? 60 * 60_000;
    const result = await pool.query<{ id: string }>(
      `INSERT INTO auctions (
         owner_id, title, description, currency_code, starts_at, ends_at,
         starting_price_minor, minimum_bid_increment_minor, cancelled_at, closed_at
       )
       VALUES ($1, 'Vintage clock', 'A mechanical clock.', 'USD',
               clock_timestamp() + ($2::double precision * interval '1 millisecond'),
               clock_timestamp() + ($3::double precision * interval '1 millisecond'),
               $4, $5,
               CASE WHEN $6 THEN clock_timestamp() ELSE NULL END,
               -- Derived from ends_at (not app-clock time) so this never races
               -- the auctions_closed_at_check constraint under clock skew.
               CASE WHEN $7 THEN
                 (clock_timestamp() + ($3::double precision * interval '1 millisecond'))
                 + interval '1 second'
               ELSE NULL END)
       RETURNING id::text`,
      [
        ownerId,
        startsAtOffsetMs,
        endsAtOffsetMs,
        options.startingPriceMinor ?? "1000",
        options.minimumBidIncrementMinor ?? "100",
        options.cancelled === true,
        options.closed === true,
      ],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error("auction fixture was not inserted");
    }
    return row.id;
  }

  it("accepts the first bid at exactly the starting price and updates the auction", async () => {
    const auctionId = await insertAuction({ startingPriceMinor: "1000" });

    const outcome = await repository.placeBid({
      auctionId,
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });

    expect(outcome).toMatchObject({
      kind: "placed",
      bid: { auctionId, bidderId, amountMinor: "1000" },
      auction: { id: auctionId, version: "2" },
    });
    if (outcome.kind !== "placed") {
      throw new Error("expected placed");
    }
    expect(outcome.auction.leadingBidId).toBe(outcome.bid.id);

    const auctionRow = await pool.query<{ leading_bid_id: string; version: string }>(
      `SELECT leading_bid_id::text, version::text FROM auctions WHERE id = $1`,
      [auctionId],
    );
    expect(auctionRow.rows[0]).toEqual({
      leading_bid_id: outcome.bid.id,
      version: "2",
    });

    const eventResult = await pool.query<{
      event_id: string;
      event_type: string;
      occurred_at: Date;
      auction_id: string;
      auction_version: string;
      payload: Record<string, unknown>;
      published_at: Date | null;
    }>(
      `SELECT event_id::text, event_type, occurred_at, auction_id::text,
              auction_version::text, payload, published_at
       FROM outbox_events
       WHERE auction_id = $1`,
      [auctionId],
    );
    expect(eventResult.rows).toHaveLength(1);
    expect(eventResult.rows[0]).toMatchObject({
      event_type: "BidAccepted.v1",
      auction_id: auctionId,
      auction_version: "2",
      published_at: null,
      payload: {
        eventType: "BidAccepted.v1",
        auctionId,
        auctionVersion: "2",
        bidId: outcome.bid.id,
        amountMinor: "1000",
        currencyCode: "USD",
      },
    });
    expect(eventResult.rows[0]?.payload.eventId).toBe(eventResult.rows[0]?.event_id);
    expect(eventResult.rows[0]?.payload.occurredAt).toEqual(expect.any(String));
    expect(eventResult.rows[0]?.occurred_at.getTime()).toBe(outcome.bid.createdAt.getTime());
  });

  it("rejects a bid below the current qualifying threshold and reports it", async () => {
    const auctionId = await insertAuction({
      startingPriceMinor: "1000",
      minimumBidIncrementMinor: "100",
    });
    await repository.placeBid({
      auctionId,
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });

    const outcome = await repository.placeBid({
      auctionId,
      bidderId: otherBidderId,
      amountMinor: "1050",
      idempotencyKey: "key-2",
    });

    expect(outcome).toEqual({ kind: "bid_too_low", minimumQualifyingAmountMinor: "1100" });

    const eventCount = await pool.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM outbox_events WHERE auction_id = $1",
      [auctionId],
    );
    expect(eventCount.rows[0]?.count).toBe("1");
  });

  it("floors the qualifying threshold at 1 minor unit when the starting price is zero", async () => {
    const auctionId = await insertAuction({ startingPriceMinor: "0" });

    await expect(
      repository.placeBid({ auctionId, bidderId, amountMinor: "1", idempotencyKey: "key-1" }),
    ).resolves.toMatchObject({ kind: "placed" });
  });

  it("does not allow the owner to bid on their own auction", async () => {
    const auctionId = await insertAuction();

    const outcome = await repository.placeBid({
      auctionId,
      bidderId: ownerId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });

    expect(outcome).toEqual({ kind: "self_bid_not_allowed" });
  });

  it("rejects a bid before the auction has started", async () => {
    const auctionId = await insertAuction({
      startsAtOffsetMs: 60 * 60_000,
      endsAtOffsetMs: 2 * 60 * 60_000,
    });

    const outcome = await repository.placeBid({
      auctionId,
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });

    expect(outcome).toEqual({ kind: "auction_not_open", phase: "scheduled" });
  });

  it("rejects a bid once the auction has ended", async () => {
    const auctionId = await insertAuction({
      startsAtOffsetMs: -2 * 60 * 60_000,
      endsAtOffsetMs: -60 * 60_000,
    });

    const outcome = await repository.placeBid({
      auctionId,
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });

    expect(outcome).toEqual({ kind: "auction_not_open", phase: "awaiting_finalization" });
  });

  it("rejects a bid on a cancelled auction", async () => {
    const auctionId = await insertAuction({ cancelled: true });

    const outcome = await repository.placeBid({
      auctionId,
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });

    expect(outcome).toEqual({ kind: "auction_not_open", phase: "cancelled" });
  });

  it("rejects a bid on a closed auction", async () => {
    const auctionId = await insertAuction({
      startsAtOffsetMs: -2 * 60 * 60_000,
      endsAtOffsetMs: -60 * 60_000,
      closed: true,
    });

    const outcome = await repository.placeBid({
      auctionId,
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });

    expect(outcome).toEqual({ kind: "auction_not_open", phase: "closed" });
  });

  it("reports auction_not_found for a nonexistent auction", async () => {
    const outcome = await repository.placeBid({
      auctionId: "9223372036854775807",
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });

    expect(outcome).toEqual({ kind: "auction_not_found" });
  });

  it("serializes two racing bids on the same auction through the row lock", async () => {
    const auctionId = await insertAuction({
      startingPriceMinor: "1000",
      minimumBidIncrementMinor: "100",
    });

    const [first, second] = await Promise.all([
      repository.placeBid({
        auctionId,
        bidderId,
        amountMinor: "1000",
        idempotencyKey: "race-a",
      }),
      repository.placeBid({
        auctionId,
        bidderId: otherBidderId,
        amountMinor: "1000",
        idempotencyKey: "race-b",
      }),
    ]);

    const outcomes = [first, second];
    const placed = outcomes.filter((outcome) => outcome.kind === "placed");
    const tooLow = outcomes.filter((outcome) => outcome.kind === "bid_too_low");
    expect(placed).toHaveLength(1);
    expect(tooLow).toHaveLength(1);
    if (tooLow[0]!.kind !== "bid_too_low") {
      throw new Error("expected bid_too_low");
    }
    expect(tooLow[0]!.minimumQualifyingAmountMinor).toBe("1100");

    const auctionRow = await pool.query<{ version: string }>(
      `SELECT version::text FROM auctions WHERE id = $1`,
      [auctionId],
    );
    expect(auctionRow.rows[0]?.version).toBe("2");
  });

  it("replays an identical idempotency key without creating a second bid", async () => {
    const auctionId = await insertAuction();
    const first = await repository.placeBid({
      auctionId,
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });
    if (first.kind !== "placed") {
      throw new Error("expected placed");
    }

    const replay = await repository.placeBid({
      auctionId,
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });

    expect(replay).toEqual({
      kind: "idempotency_replay",
      bid: first.bid,
      auction: expect.objectContaining({
        id: auctionId,
        version: "2",
        leadingBidId: first.bid.id,
      }),
    });

    const countResult = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM bids WHERE auction_id = $1`,
      [auctionId],
    );
    expect(countResult.rows[0]?.count).toBe("1");

    const eventCount = await pool.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM outbox_events WHERE auction_id = $1",
      [auctionId],
    );
    expect(eventCount.rows[0]?.count).toBe("1");

    const auctionRow = await pool.query<{ version: string }>(
      `SELECT version::text FROM auctions WHERE id = $1`,
      [auctionId],
    );
    expect(auctionRow.rows[0]?.version).toBe("2");
  });

  it("rejects reusing an idempotency key with a different amount without mutating the auction", async () => {
    const auctionId = await insertAuction();
    await repository.placeBid({
      auctionId,
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });

    const outcome = await repository.placeBid({
      auctionId,
      bidderId,
      amountMinor: "1200",
      idempotencyKey: "key-1",
    });

    expect(outcome).toEqual({ kind: "idempotency_key_reused", originalAmountMinor: "1000" });

    const auctionRow = await pool.query<{ version: string }>(
      `SELECT version::text FROM auctions WHERE id = $1`,
      [auctionId],
    );
    expect(auctionRow.rows[0]?.version).toBe("2");
  });

  it("resolves concurrent identical replays without a unique-constraint error and only one row", async () => {
    const auctionId = await insertAuction();

    const results = await Promise.all([
      repository.placeBid({
        auctionId,
        bidderId,
        amountMinor: "1000",
        idempotencyKey: "concurrent-key",
      }),
      repository.placeBid({
        auctionId,
        bidderId,
        amountMinor: "1000",
        idempotencyKey: "concurrent-key",
      }),
    ]);

    // The row lock fully serializes the two calls: whichever acquires it
    // first inserts and commits before the other even reads, so exactly one
    // sees a fresh placement and the other sees its own replay.
    expect(results.map((outcome) => outcome.kind).sort()).toEqual(["idempotency_replay", "placed"]);

    const countResult = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM bids WHERE auction_id = $1 AND idempotency_key = $2`,
      [auctionId, "concurrent-key"],
    );
    expect(countResult.rows[0]?.count).toBe("1");

    const eventCount = await pool.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM outbox_events WHERE auction_id = $1",
      [auctionId],
    );
    expect(eventCount.rows[0]?.count).toBe("1");
  });

  it("rolls back the bid and auction update when outbox insertion fails", async () => {
    const auctionId = await insertAuction();
    const failingDatabase: Database = {
      query: async (text, values) => database.query(text, values),
      connect: async () => {
        const client = await database.connect();
        return {
          query: async (text, values) => {
            if (text.includes("INSERT INTO outbox_events")) {
              const invalidValues = [...(values ?? [])];
              invalidValues[0] = "not-a-uuid";
              return client.query(text, invalidValues);
            }
            return client.query(text, values);
          },
          release: () => client.release(),
        };
      },
    };

    await expect(
      new PostgresBidRepository(failingDatabase).placeBid({
        auctionId,
        bidderId,
        amountMinor: "1000",
        idempotencyKey: "outbox-failure",
      }),
    ).rejects.toThrow("invalid input syntax for type uuid");

    const persisted = await pool.query<{
      version: string;
      leading_bid_id: string | null;
      bid_count: string;
      event_count: string;
    }>(
      `SELECT auctions.version::text,
              auctions.leading_bid_id::text,
              (SELECT count(*)::text FROM bids WHERE auction_id = auctions.id) AS bid_count,
              (SELECT count(*)::text FROM outbox_events WHERE auction_id = auctions.id) AS event_count
       FROM auctions
       WHERE auctions.id = $1`,
      [auctionId],
    );
    expect(persisted.rows[0]).toEqual({
      version: "1",
      leading_bid_id: null,
      bid_count: "0",
      event_count: "0",
    });
  });

  it("increments version by exactly one per accepted bid", async () => {
    const auctionId = await insertAuction({
      startingPriceMinor: "1000",
      minimumBidIncrementMinor: "100",
    });

    const amounts = ["1000", "1100", "1200", "1300"];
    // Sequential and awaited: each bid must observe the previous one's price,
    // so these cannot run concurrently.
    let index = 0;
    for (const amountMinor of amounts) {
      await repository.placeBid({
        auctionId,
        bidderId: index % 2 === 0 ? bidderId : otherBidderId,
        amountMinor,
        idempotencyKey: `sequence-${amountMinor}`,
      });
      index += 1;
    }

    const auctionRow = await pool.query<{ version: string }>(
      `SELECT version::text FROM auctions WHERE id = $1`,
      [auctionId],
    );
    expect(auctionRow.rows[0]?.version).toBe(String(1 + amounts.length));
  });

  it("stamps the bid's created_at with the same instant as the auction's resulting updated_at", async () => {
    const auctionId = await insertAuction();

    const outcome = await repository.placeBid({
      auctionId,
      bidderId,
      amountMinor: "1000",
      idempotencyKey: "key-1",
    });
    if (outcome.kind !== "placed") {
      throw new Error("expected placed");
    }

    const auctionRow = await pool.query<{ updated_at: Date }>(
      `SELECT updated_at FROM auctions WHERE id = $1`,
      [auctionId],
    );
    expect(auctionRow.rows[0]?.updated_at.getTime()).toBe(outcome.bid.createdAt.getTime());
  });

  // The next two tests hold the auction row lock from a separate client while
  // placeBid blocks on it, then change the world before releasing. They pin
  // the "lock in one statement, read in the next" structure: a single
  // lock-and-read statement captures clock_timestamp() and its snapshot before
  // the wait, so it would see neither the passed deadline nor the new bid.

  it("rejects a bid whose deadline passes while it waits for the auction lock", async () => {
    const auctionId = await insertAuction({ endsAtOffsetMs: 800 });
    const holder = await pool.connect();
    try {
      await holder.query("BEGIN");
      await holder.query("SELECT id FROM auctions WHERE id = $1 FOR UPDATE", [auctionId]);

      const blocked = repository.placeBid({
        auctionId,
        bidderId,
        amountMinor: "1000",
        idempotencyKey: "late",
      });
      // Let the deadline pass while the bid is still blocked on the lock.
      await new Promise((resolve) => setTimeout(resolve, 1200));
      await holder.query("COMMIT");

      await expect(blocked).resolves.toEqual({
        kind: "auction_not_open",
        phase: "awaiting_finalization",
      });
    } finally {
      holder.release();
    }
    const count = await pool.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM bids WHERE auction_id = $1",
      [auctionId],
    );
    expect(count.rows[0]?.count).toBe("0");
  });

  it("observes a leading bid committed while it waits for the auction lock", async () => {
    const auctionId = await insertAuction({
      startingPriceMinor: "1000",
      minimumBidIncrementMinor: "100",
    });
    const holder = await pool.connect();
    try {
      await holder.query("BEGIN");
      await holder.query("SELECT id FROM auctions WHERE id = $1 FOR UPDATE", [auctionId]);

      const blocked = repository.placeBid({
        auctionId,
        bidderId: otherBidderId,
        amountMinor: "1000",
        idempotencyKey: "blocked",
      });
      await new Promise((resolve) => setTimeout(resolve, 300));

      // Accept a higher bid on the holder's connection, then release the lock.
      const inserted = await holder.query<{ id: string }>(
        `INSERT INTO bids (auction_id, bidder_id, amount_minor, idempotency_key, created_at)
         VALUES ($1, $2, 1500, 'holder', clock_timestamp())
         RETURNING id::text`,
        [auctionId, bidderId],
      );
      await holder.query(
        "UPDATE auctions SET leading_bid_id = $1, version = version + 1 WHERE id = $2",
        [inserted.rows[0]!.id, auctionId],
      );
      // Every version bump needs its outbox event by commit.
      await holder.query(
        `INSERT INTO outbox_events (
           event_id, event_type, occurred_at, auction_id, auction_version, payload
         )
         SELECT event.id, 'BidAccepted.v1', event.at, auctions.id, auctions.version,
                jsonb_build_object(
                  'eventId', event.id::text,
                  'eventType', 'BidAccepted.v1',
                  'occurredAt',
                    to_char(event.at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                  'auctionId', auctions.id::text,
                  'auctionVersion', auctions.version::text,
                  'bidId', $2::text,
                  'amountMinor', '1500',
                  'currencyCode', auctions.currency_code
                )
         FROM auctions
         CROSS JOIN (SELECT gen_random_uuid() AS id, clock_timestamp() AS at) AS event
         WHERE auctions.id = $1`,
        [auctionId, inserted.rows[0]!.id],
      );
      await holder.query("COMMIT");

      await expect(blocked).resolves.toEqual({
        kind: "bid_too_low",
        minimumQualifyingAmountMinor: "1600",
      });
    } finally {
      holder.release();
    }
  });
});
