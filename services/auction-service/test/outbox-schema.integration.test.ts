import { randomUUID } from "node:crypto";

import { Pool, type PoolClient } from "pg";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

const databaseUrl = process.env.TEST_DATABASE_URL;

interface OutboxEventInsert {
  eventId: string;
  eventType: string;
  occurredAt: string;
  auctionId: string;
  auctionVersion: string;
  payload: Record<string, unknown>;
  publishedAt: string | null;
}

describe.skipIf(databaseUrl === undefined)("outbox PostgreSQL schema", () => {
  let database: Pool;
  let ownerId: string;
  let auctionId: string;

  function validEvent(overrides: Partial<OutboxEventInsert> = {}): OutboxEventInsert {
    const eventId = overrides.eventId ?? randomUUID();
    const eventType = overrides.eventType ?? "BidAccepted.v1";
    const occurredAt = overrides.occurredAt ?? "2030-01-01T10:00:00.123456Z";
    const eventAuctionId = overrides.auctionId ?? auctionId;
    // A fresh fixture auction is at version 1, the only version it accepts.
    const auctionVersion = overrides.auctionVersion ?? "1";

    return {
      eventId,
      eventType,
      occurredAt,
      auctionId: eventAuctionId,
      auctionVersion,
      payload: overrides.payload ?? {
        eventId,
        eventType,
        occurredAt,
        auctionId: eventAuctionId,
        auctionVersion,
        bidId: "10",
        amountMinor: "1000",
        currencyCode: "USD",
      },
      publishedAt: overrides.publishedAt ?? null,
    };
  }

  async function insertEvent(
    overrides: Partial<OutboxEventInsert> = {},
    queryable: Pool | PoolClient = database,
  ): Promise<string> {
    const event = validEvent(overrides);
    await queryable.query(
      `INSERT INTO outbox_events (
         event_id, event_type, occurred_at, auction_id, auction_version, payload, published_at
       )
       VALUES ($1::uuid, $2, $3::timestamptz, $4, $5, $6::jsonb, $7::timestamptz)`,
      [
        event.eventId,
        event.eventType,
        event.occurredAt,
        event.auctionId,
        event.auctionVersion,
        JSON.stringify(event.payload),
        event.publishedAt,
      ],
    );
    return event.eventId;
  }

  beforeAll(() => {
    database = new Pool({ connectionString: databaseUrl });
  });

  beforeEach(async () => {
    const ownerResult = await database.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'Outbox Owner', '$argon2id$test')
       RETURNING id::text`,
      [`outbox-schema-owner-${randomUUID()}@example.com`],
    );
    const owner = ownerResult.rows[0];
    if (owner === undefined) {
      throw new Error("owner fixture was not inserted");
    }
    ownerId = owner.id;

    const auctionResult = await database.query<{ id: string }>(
      `INSERT INTO auctions (
         owner_id, title, description, currency_code, starts_at, ends_at,
         starting_price_minor, minimum_bid_increment_minor
       )
       VALUES ($1, 'Outbox lot', 'An outbox schema fixture.', 'USD',
               now(), now() + interval '1 hour', 1000, 100)
       RETURNING id::text`,
      [ownerId],
    );
    const auction = auctionResult.rows[0];
    if (auction === undefined) {
      throw new Error("auction fixture was not inserted");
    }
    auctionId = auction.id;
  });

  afterEach(async () => {
    await database.query("DELETE FROM outbox_events WHERE auction_id = $1", [auctionId]);
    await database.query("DELETE FROM auctions WHERE id = $1", [auctionId]);
    await database.query("DELETE FROM users WHERE id = $1", [ownerId]);
  });

  afterAll(async () => {
    await database.end();
  });

  it("allows publication state to change while event data remains immutable", async () => {
    const eventId = await insertEvent();

    await expect(
      database.query(
        "UPDATE outbox_events SET published_at = occurred_at WHERE event_id = $1::uuid",
        [eventId],
      ),
    ).resolves.toMatchObject({ rowCount: 1 });

    const immutableUpdates: [string, readonly unknown[]][] = [
      ["payload = payload || '{\"extra\": true}'::jsonb", []],
      ["auction_version = auction_version + 1", []],
      ["occurred_at = occurred_at + interval '1 second'", []],
      ["event_id = $2::uuid", [randomUUID()]],
    ];
    for (const [assignment, additionalValues] of immutableUpdates) {
      await expect(
        database.query(`UPDATE outbox_events SET ${assignment} WHERE event_id = $1::uuid`, [
          eventId,
          ...additionalValues,
        ]),
      ).rejects.toMatchObject({
        code: "23000",
        message: "outbox event data is immutable",
      });
    }
  });

  it("rejects publication timestamps before the event occurred", async () => {
    await expect(insertEvent({ publishedAt: "2030-01-01T10:00:00.123455Z" })).rejects.toMatchObject(
      {
        code: "23514",
        constraint: "outbox_events_published_at_check",
      },
    );
  });

  it("requires payload metadata to match the queryable columns", async () => {
    const event = validEvent();
    await expect(
      insertEvent({
        eventId: event.eventId,
        payload: { ...event.payload, auctionVersion: "3" },
      }),
    ).rejects.toMatchObject({
      code: "23514",
      constraint: "outbox_events_payload_metadata_check",
    });
  });

  it("allows exactly one event per auction version, whatever its type", async () => {
    await insertEvent();

    await expect(insertEvent()).rejects.toMatchObject({
      code: "23505",
      constraint: "outbox_events_auction_version_key",
    });
    // Two types at one version would have no defined publication order.
    await expect(insertEvent({ eventType: "AuctionCancelled.v1" })).rejects.toMatchObject({
      code: "23505",
      constraint: "outbox_events_auction_version_key",
    });
  });

  it("accepts only events for the auction's current version", async () => {
    for (const auctionVersion of ["2", "3"]) {
      await expect(insertEvent({ auctionVersion })).rejects.toMatchObject({
        code: "23514",
        constraint: "outbox_events_current_auction_version",
      });
    }

    // A version bump must commit together with its event, so both happen in
    // one transaction; the event for the bumped version is the only one it
    // accepts.
    const writer = await database.connect();
    try {
      await writer.query("BEGIN");
      await writer.query("UPDATE auctions SET version = 2 WHERE id = $1", [auctionId]);
      for (const auctionVersion of ["1", "3"]) {
        await writer.query("SAVEPOINT attempt");
        await expect(insertEvent({ auctionVersion }, writer)).rejects.toMatchObject({
          code: "23514",
          constraint: "outbox_events_current_auction_version",
        });
        await writer.query("ROLLBACK TO SAVEPOINT attempt");
      }
      await expect(insertEvent({ auctionVersion: "2" }, writer)).resolves.toBeTypeOf("string");
      await writer.query("COMMIT");
    } finally {
      writer.release();
    }
  });

  it("holds the auction row lock from the event insert until commit", async () => {
    const writer = await database.connect();
    const competitor = await database.connect();
    try {
      await writer.query("BEGIN");
      await insertEvent({}, writer);

      // Any other writer for this auction (which must lock it to bump its
      // version) waits until the event's transaction has committed.
      await expect(
        competitor.query("SELECT 1 FROM auctions WHERE id = $1 FOR NO KEY UPDATE NOWAIT", [
          auctionId,
        ]),
      ).rejects.toMatchObject({ code: "55P03" });
    } finally {
      await writer.query("ROLLBACK");
      writer.release();
      competitor.release();
    }
  });

  it("indexes only unpublished events in claim order", async () => {
    const result = await database.query<{ indexdef: string }>(
      `SELECT indexdef
       FROM pg_indexes
       WHERE schemaname = 'public' AND indexname = 'outbox_events_unpublished_idx'`,
    );

    expect(result.rows[0]?.indexdef).toContain("(occurred_at, event_id)");
    expect(result.rows[0]?.indexdef).toContain("WHERE (published_at IS NULL)");
  });
});
