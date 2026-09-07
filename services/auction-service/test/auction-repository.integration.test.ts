import { randomUUID } from "node:crypto";

import { Pool } from "pg";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { PostgresAuctionRepository } from "../src/auctions/auction.repository.js";
import { AuctionService } from "../src/auctions/auction.service.js";
import type { Database } from "../src/database.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(databaseUrl === undefined)("auction creation PostgreSQL transaction", () => {
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
  });
});
