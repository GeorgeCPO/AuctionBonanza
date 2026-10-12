import { randomUUID } from "node:crypto";

import { Pool } from "pg";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { PostgresAuctionRepository } from "../src/auctions/auction.repository.js";
import { AuctionService } from "../src/auctions/auction.service.js";
import type { Database } from "../src/database.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(databaseUrl === undefined)("auction PostgreSQL repository", () => {
  let pool: Pool;
  let database: Database;
  let repository: PostgresAuctionRepository;
  let service: AuctionService;
  let ownerId: string;

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
    repository = new PostgresAuctionRepository(database);
    service = new AuctionService(repository, ["USD"]);
  });

  beforeEach(async () => {
    const result = await pool.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'Auction Owner', '$argon2id$test')
       RETURNING id::text`,
      [`auction-api-${randomUUID()}@example.com`],
    );
    const owner = result.rows[0];
    if (owner === undefined) throw new Error("owner fixture was not inserted");
    ownerId = owner.id;
  });

  afterEach(async () => {
    await pool.query("DELETE FROM auctions WHERE owner_id = $1", [ownerId]);
    await pool.query("DELETE FROM users WHERE id = $1", [ownerId]);
  });

  afterAll(async () => {
    await pool.end();
  });

  function futureInput(title = "Vintage clock") {
    const startsAt = new Date(Date.now() + 10 * 60_000);
    const endsAt = new Date(startsAt.getTime() + 60 * 60_000);
    return {
      title,
      description: "A mechanical clock.",
      currencyCode: "USD",
      startsAt: startsAt.toISOString(),
      endsAt: endsAt.toISOString(),
      startingPriceMinor: "1000",
      minimumBidIncrementMinor: "100",
    };
  }

  it.each([1, 2, 3, 20])(
    "traverses an unchanged dataset with page size %i without duplicates or omissions",
    async (limit) => {
      const search = `pagination-${randomUUID()}`;
      // IDs deliberately disagree with timestamp order. All times collapse to the
      // same JS millisecond, and two rows have exactly the same PostgreSQL time.
      const fixtures = [
        { id: "9007199254740993", time: "2025-01-01T00:00:00.123456Z" },
        { id: "9007199254740995", time: "2025-01-01T00:00:00.123456Z" },
        { id: "9007199254740997", time: "2025-01-01T00:00:00.123455Z" },
        { id: "9007199254740999", time: "2025-01-01T00:00:00.123001Z" },
      ];
      for (const fixture of fixtures) {
        await pool.query(
          `INSERT INTO auctions (
             id, owner_id, title, description, currency_code, starts_at, ends_at,
             starting_price_minor, minimum_bid_increment_minor, created_at, updated_at
           ) OVERRIDING SYSTEM VALUE
           VALUES ($1, $2, $3, 'Pagination fixture', 'USD',
                   now(), now() + interval '1 hour', 0, 1, $4, $4)`,
          [fixture.id, ownerId, search, fixture.time],
        );
      }

      const expected = [fixtures[1]!, fixtures[0]!, fixtures[2]!, fixtures[3]!];
      const ids: string[] = [];
      let cursor: string | undefined;
      for (let pageNumber = 0; pageNumber < 5; pageNumber += 1) {
        const result = await service.list({ search, limit: String(limit), cursor });
        if (result.kind !== "found") throw new Error("Expected a page");
        const { items, nextCursor } = result.page;
        expect(items.length).toBe(Math.min(limit, fixtures.length - ids.length));
        ids.push(...items.map((item) => item.id));
        expect(nextCursor === null).toBe(ids.length === fixtures.length);
        if (ids.length === fixtures.length) {
          break;
        }
        expect(nextCursor).not.toBeNull();
        const position: unknown = JSON.parse(Buffer.from(nextCursor!, "base64url").toString());
        expect(position).toEqual({
          createdAt: expected[ids.length - 1]!.time,
          id: expected[ids.length - 1]!.id,
        });
        cursor = nextCursor!;
      }
      expect(ids).toEqual(expected.map((fixture) => fixture.id));
      expect(new Set(ids).size).toBe(fixtures.length);
    },
  );

  it("matches title substrings case-insensitively with literal wildcard and SQL characters", async () => {
    const prefix = `search-${randomUUID()}`;
    const titles = [
      `${prefix} Vintage CLOCK`,
      `${prefix} 100%_\\' clock`,
      `${prefix} 100xy clock`,
      `${prefix} unrelated`,
      `${prefix} code_ clock`,
      `${prefix} codeX clock`,
    ];
    for (const title of titles) {
      const outcome = await service.create(ownerId, {
        ...futureInput(title),
        description: `${prefix} description-only needle`,
      });
      expect(outcome.kind).toBe("created");
    }
    const searches = [
      { term: `  ${prefix.toUpperCase()} VINTAGE cl  `, titles: [titles[0]] },
      { term: `${prefix} 100%`, titles: [titles[1]] },
      { term: `${prefix} code_`, titles: [titles[4]] },
      { term: `${prefix} 100%_\\'`, titles: [titles[1]] },
      { term: `${prefix} description-only`, titles: [] },
      { term: `${prefix}' OR TRUE --`, titles: [] },
    ];
    for (const search of searches) {
      const result = await service.list({ search: search.term });
      if (result.kind !== "found") throw new Error("Expected a page");
      expect(result.page.items.map((item) => item.title)).toEqual(search.titles);
      expect(result.page.nextCursor).toBeNull();
    }
  });

  it("creates separate auctions for repeated submissions", async () => {
    const input = futureInput();
    const first = await service.create(ownerId, input);
    const second = await service.create(ownerId, input);
    if (first.kind !== "created" || second.kind !== "created") {
      throw new Error("Expected two successful creations");
    }
    expect(second.auction.id).not.toBe(first.auction.id);
    await expect(repository.findById(first.auction.id)).resolves.toMatchObject({
      ownerId,
      title: input.title,
      phase: "scheduled",
    });
  });

  it("defaults an omitted start to database time", async () => {
    const { startsAt: _startsAt, ...input } = futureInput();
    const before = await pool.query<{ time: Date }>("SELECT clock_timestamp() AS time");
    const outcome = await service.create(ownerId, input);
    const after = await pool.query<{ time: Date }>("SELECT clock_timestamp() AS time");
    if (outcome.kind !== "created") throw new Error("Expected successful creation");
    expect(outcome.auction.phase).toBe("open");
    expect(outcome.auction.startsAt.getTime()).toBeGreaterThanOrEqual(
      before.rows[0]!.time.getTime(),
    );
    expect(outcome.auction.startsAt.getTime()).toBeLessThanOrEqual(after.rows[0]!.time.getTime());
  });

  it.each([0, -60_000, 59_999, 30 * 24 * 60 * 60_000 + 1])(
    "rejects a duration of %i ms and accepts a subsequent valid creation",
    async (duration) => {
      const input = futureInput();
      const invalid = {
        ...input,
        endsAt: new Date(Date.parse(input.startsAt) + duration).toISOString(),
      };
      await expect(service.create(ownerId, invalid)).resolves.toMatchObject({
        kind: "invalid",
      });
      await expect(service.create(ownerId, input)).resolves.toMatchObject({
        kind: "created",
      });
    },
  );

  it("accepts a start in the past when the duration is valid", async () => {
    const pastStart = new Date(Date.now() - 2 * 60_000);
    const input = {
      ...futureInput(),
      startsAt: pastStart.toISOString(),
      endsAt: new Date(pastStart.getTime() + 60 * 60_000).toISOString(),
    };

    await expect(service.create(ownerId, input)).resolves.toMatchObject({
      kind: "created",
      auction: { startsAt: pastStart, phase: "open" },
    });
  });

  it("derives all lifecycle phases from database time with terminal facts taking precedence", async () => {
    const result = await pool.query<{ id: string; expected_phase: string }>(
      `INSERT INTO auctions (
         owner_id, title, description, currency_code, starts_at, ends_at,
         starting_price_minor, minimum_bid_increment_minor, cancelled_at, closed_at
       )
       VALUES
         ($1, 'Scheduled', 'Scheduled auction', 'USD',
          clock_timestamp() + interval '10 minutes', clock_timestamp() + interval '20 minutes',
          0, 1, NULL, NULL),
         ($1, 'Open', 'Open auction', 'USD',
          clock_timestamp() - interval '10 minutes', clock_timestamp() + interval '10 minutes',
          0, 1, NULL, NULL),
         ($1, 'Awaiting', 'Awaiting auction', 'USD',
          clock_timestamp() - interval '20 minutes', clock_timestamp() - interval '10 minutes',
          0, 1, NULL, NULL),
         ($1, 'Cancelled', 'Cancelled auction', 'USD',
          clock_timestamp() - interval '20 minutes', clock_timestamp() - interval '10 minutes',
          0, 1, clock_timestamp(), NULL),
         ($1, 'Closed', 'Closed auction', 'USD',
          clock_timestamp() - interval '20 minutes', clock_timestamp() - interval '10 minutes',
          0, 1, NULL, clock_timestamp())
       RETURNING id::text,
         CASE title
           WHEN 'Scheduled' THEN 'scheduled'
           WHEN 'Open' THEN 'open'
           WHEN 'Awaiting' THEN 'awaiting_finalization'
           WHEN 'Cancelled' THEN 'cancelled'
           ELSE 'closed'
         END AS expected_phase`,
      [ownerId],
    );

    for (const row of result.rows) {
      await expect(repository.findById(row.id)).resolves.toMatchObject({
        id: row.id,
        phase: row.expected_phase,
      });
    }
    const entries = await repository.list({ limit: 100 });
    for (const row of result.rows) {
      expect(entries.find((entry) => entry.auction.id === row.id)?.auction.phase).toBe(
        row.expected_phase,
      );
    }
  });

  it("reads the winning bid of a closed auction and null for an open or unsold one", async () => {
    const bidderResult = await pool.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'Auction Bidder', '$argon2id$test')
       RETURNING id::text`,
      [`auction-bidder-${randomUUID()}@example.com`],
    );
    const bidderId = bidderResult.rows[0]!.id;
    const auctions = await pool.query<{ id: string; title: string }>(
      `INSERT INTO auctions (
         owner_id, title, description, currency_code, starts_at, ends_at,
         starting_price_minor, minimum_bid_increment_minor
       )
       VALUES
         ($1, 'Sold', 'Closed with a winner', 'USD',
          clock_timestamp() - interval '20 minutes', clock_timestamp() - interval '10 minutes', 0, 1),
         ($1, 'Unsold', 'Closed without a winner', 'USD',
          clock_timestamp() - interval '20 minutes', clock_timestamp() - interval '10 minutes', 0, 1),
         ($1, 'Open', 'Still open', 'USD',
          clock_timestamp() - interval '10 minutes', clock_timestamp() + interval '10 minutes', 0, 1)
       RETURNING id::text, title`,
      [ownerId],
    );
    const idOf = (title: string): string => auctions.rows.find((row) => row.title === title)!.id;
    try {
      const bid = await pool.query<{ id: string }>(
        `INSERT INTO bids (auction_id, bidder_id, amount_minor, idempotency_key, created_at)
         VALUES ($1, $2, 500, 'repository-test', clock_timestamp() - interval '15 minutes')
         RETURNING id::text`,
        [idOf("Sold"), bidderId],
      );
      const winningBidId = bid.rows[0]!.id;
      // No version step, so no outbox event is needed for these direct writes.
      await pool.query(
        `UPDATE auctions
         SET leading_bid_id = $2, closed_at = clock_timestamp(), winning_bid_id = $2
         WHERE id = $1`,
        [idOf("Sold"), winningBidId],
      );
      await pool.query("UPDATE auctions SET closed_at = clock_timestamp() WHERE id = $1", [
        idOf("Unsold"),
      ]);

      const expected = new Map([
        [idOf("Sold"), winningBidId],
        [idOf("Unsold"), null],
        [idOf("Open"), null],
      ]);
      const entries = await repository.list({ limit: 100 });
      for (const [auctionId, winner] of expected) {
        await expect(repository.findById(auctionId)).resolves.toMatchObject({
          winningBidId: winner,
        });
        expect(entries.find((entry) => entry.auction.id === auctionId)?.auction.winningBidId).toBe(
          winner,
        );
      }
    } finally {
      // A closed auction's outcome is immutable, so its leading bid cannot be
      // cleared first. Deleting the bid and the auction in one statement is
      // fine: foreign keys are checked at the end of the statement.
      await pool.query(
        `WITH removed AS (DELETE FROM bids WHERE auction_id = $1 RETURNING id)
         DELETE FROM auctions WHERE id = $1`,
        [idOf("Sold")],
      );
      await pool.query("DELETE FROM users WHERE id = $1", [bidderId]);
    }
  });
});
