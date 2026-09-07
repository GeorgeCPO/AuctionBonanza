import { randomUUID } from "node:crypto";

import { Pool } from "pg";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

const databaseUrl = process.env.TEST_DATABASE_URL;

interface AuctionInsert {
  ownerId: string | null;
  title: string | null;
  description: string | null;
  currencyCode: string | null;
  startsAt: string | null;
  endsAt: string | null;
  startingPriceMinor: string | null;
  minimumBidIncrementMinor: string | null;
  version: string | null;
  cancelledAt: string | null;
  closedAt: string | null;
  createdAt: string | null;
  updatedAt: string | null;
}

describe.skipIf(databaseUrl === undefined)("auction PostgreSQL schema", () => {
  let database: Pool;
  let ownerId: string;

  function validAuction(overrides: Partial<AuctionInsert> = {}): AuctionInsert {
    return {
      ownerId,
      title: "Vintage clock",
      description: "A mechanical mantel clock in working condition.",
      currencyCode: "USD",
      startsAt: "2030-01-01T10:00:00.000Z",
      endsAt: "2030-01-01T11:00:00.000Z",
      startingPriceMinor: "1000",
      minimumBidIncrementMinor: "100",
      version: "1",
      cancelledAt: null,
      closedAt: null,
      createdAt: "2030-01-01T09:00:00.000Z",
      updatedAt: "2030-01-01T09:00:00.000Z",
      ...overrides,
    };
  }

  async function insertAuction(overrides: Partial<AuctionInsert> = {}): Promise<void> {
    const auction = validAuction(overrides);
    await database.query(
      `INSERT INTO auctions (
         owner_id,
         title,
         description,
         currency_code,
         starts_at,
         ends_at,
         starting_price_minor,
         minimum_bid_increment_minor,
         version,
         cancelled_at,
         closed_at,
         created_at,
         updated_at
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
      [
        auction.ownerId,
        auction.title,
        auction.description,
        auction.currencyCode,
        auction.startsAt,
        auction.endsAt,
        auction.startingPriceMinor,
        auction.minimumBidIncrementMinor,
        auction.version,
        auction.cancelledAt,
        auction.closedAt,
        auction.createdAt,
        auction.updatedAt,
      ],
    );
  }

  async function expectCheckViolation(
    constraint: string,
    overrides: Partial<AuctionInsert>,
  ): Promise<void> {
    await expect(insertAuction(overrides)).rejects.toMatchObject({
      code: "23514",
      constraint,
    });
  }

  beforeAll(() => {
    database = new Pool({ connectionString: databaseUrl });
  });

  beforeEach(async () => {
    const result = await database.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'Auction Owner', '$argon2id$test')
       RETURNING id::text`,
      [`auction-schema-${randomUUID()}@example.com`],
    );
    const insertedOwner = result.rows[0];
    if (insertedOwner === undefined) throw new Error("owner fixture was not inserted");
    ownerId = insertedOwner.id;
  });

  afterEach(async () => {
    await database.query("DELETE FROM auctions WHERE owner_id = $1", [ownerId]);
    await database.query("DELETE FROM users WHERE id = $1", [ownerId]);
  });

  afterAll(async () => {
    await database.end();
  });

  it("accepts the inclusive duration limits and legal terminal states", async () => {
    await insertAuction({
      endsAt: "2030-01-01T10:01:00.000Z",
      cancelledAt: "2030-01-01T09:30:00.000Z",
    });
    await insertAuction({
      endsAt: "2030-01-31T10:00:00.000Z",
      closedAt: "2030-01-31T10:00:00.000Z",
    });

    const result = await database.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM auctions WHERE owner_id = $1",
      [ownerId],
    );
    expect(result.rows[0]?.count).toBe("2");
  });

  it("applies version and timestamp defaults while leaving terminal facts empty", async () => {
    const result = await database.query<{
      version: string;
      created_at: Date;
      updated_at: Date;
      cancelled_at: Date | null;
      closed_at: Date | null;
    }>(
      `INSERT INTO auctions (
         owner_id,
         title,
         description,
         currency_code,
         starts_at,
         ends_at,
         starting_price_minor,
         minimum_bid_increment_minor
       )
       VALUES ($1, 'Defaulted auction', 'Defaulted auction description.', 'EUR',
               now(), now() + interval '1 hour', 0, 1)
       RETURNING version::text, created_at, updated_at, cancelled_at, closed_at`,
      [ownerId],
    );

    const auction = result.rows[0];
    expect(auction?.version).toBe("1");
    expect(auction?.updated_at).toEqual(auction?.created_at);
    expect(auction?.cancelled_at).toBeNull();
    expect(auction?.closed_at).toBeNull();
  });

  it.each([
    ["owner_id", { ownerId: null }],
    ["title", { title: null }],
    ["description", { description: null }],
    ["currency_code", { currencyCode: null }],
    ["starts_at", { startsAt: null }],
    ["ends_at", { endsAt: null }],
    ["starting_price_minor", { startingPriceMinor: null }],
    ["minimum_bid_increment_minor", { minimumBidIncrementMinor: null }],
    ["version", { version: null }],
    ["created_at", { createdAt: null }],
    ["updated_at", { updatedAt: null }],
  ] satisfies [string, Partial<AuctionInsert>][])(
    "requires the %s column",
    async (column, overrides) => {
      await expect(insertAuction(overrides)).rejects.toMatchObject({
        code: "23502",
        column,
      });
    },
  );

  it("requires an existing owner and prevents deleting an owner with auctions", async () => {
    await expect(insertAuction({ ownerId: "9223372036854775807" })).rejects.toMatchObject({
      code: "23503",
      constraint: "auctions_owner_id_fkey",
    });

    await insertAuction();
    await expect(
      database.query("DELETE FROM users WHERE id = $1", [ownerId]),
    ).rejects.toMatchObject({
      code: "23503",
      constraint: "auctions_owner_id_fkey",
    });
  });

  it.each([
    ["empty", ""],
    ["whitespace-only", "   "],
    ["leading whitespace", " Vintage clock"],
    ["trailing whitespace", "Vintage clock "],
  ])("rejects an %s title", async (_case, title) => {
    expect.hasAssertions();
    await expectCheckViolation("auctions_title_check", { title });
  });

  it("limits titles to 160 characters", async () => {
    await expect(insertAuction({ title: "t".repeat(161) })).rejects.toMatchObject({
      code: "22001",
    });
  });

  it.each([
    ["empty", ""],
    ["whitespace-only", "   "],
    ["leading whitespace", " Description"],
    ["trailing whitespace", "Description "],
    ["overlong", "d".repeat(10_001)],
  ])("rejects an %s description", async (_case, description) => {
    expect.hasAssertions();
    await expectCheckViolation("auctions_description_check", { description });
  });

  it.each(["usd", "US1", "US", "EURO"])("rejects malformed currency code %s", async (code) => {
    const expectedCode = code.length > 3 ? "22001" : "23514";
    await expect(insertAuction({ currencyCode: code })).rejects.toMatchObject({
      code: expectedCode,
    });
  });

  it("requires the end to be later than the start", async () => {
    expect.hasAssertions();
    await expectCheckViolation("auctions_schedule_check", {
      endsAt: "2030-01-01T10:00:00.000Z",
    });
  });

  it.each([
    ["shorter than one minute", "2030-01-01T10:00:59.999Z"],
    ["longer than thirty days", "2030-01-31T10:00:00.001Z"],
  ])("rejects a duration %s", async (_case, endsAt) => {
    expect.hasAssertions();
    await expectCheckViolation("auctions_duration_check", { endsAt });
  });

  it("rejects a negative starting price", async () => {
    expect.hasAssertions();
    await expectCheckViolation("auctions_starting_price_minor_check", {
      startingPriceMinor: "-1",
    });
  });

  it("requires a positive minimum bid increment", async () => {
    expect.hasAssertions();
    await expectCheckViolation("auctions_minimum_bid_increment_minor_check", {
      minimumBidIncrementMinor: "0",
    });
  });

  it("requires a positive aggregate version", async () => {
    expect.hasAssertions();
    await expectCheckViolation("auctions_version_check", { version: "0" });
  });

  it("prevents an auction from being both cancelled and closed", async () => {
    expect.hasAssertions();
    await expectCheckViolation("auctions_terminal_state_check", {
      cancelledAt: "2030-01-01T10:30:00.000Z",
      closedAt: "2030-01-01T11:00:00.000Z",
    });
  });

  it("prevents closure before the scheduled end", async () => {
    expect.hasAssertions();
    await expectCheckViolation("auctions_closed_at_check", {
      closedAt: "2030-01-01T10:59:59.999Z",
    });
  });

  it("prevents updated_at from preceding created_at", async () => {
    expect.hasAssertions();
    await expectCheckViolation("auctions_updated_at_check", {
      updatedAt: "2030-01-01T08:59:59.999Z",
    });
  });

  it("uses a partial index for auctions awaiting finalization", async () => {
    const result = await database.query<{ indexdef: string }>(
      `SELECT indexdef
       FROM pg_indexes
       WHERE schemaname = 'public'
         AND tablename = 'auctions'
         AND indexname = 'auctions_pending_finalization_idx'`,
    );

    expect(result.rows[0]?.indexdef).toContain("(ends_at, id)");
    expect(result.rows[0]?.indexdef).toContain(
      "WHERE ((cancelled_at IS NULL) AND (closed_at IS NULL))",
    );
  });
});
