import { randomUUID } from "node:crypto";

import { Pool, type PoolClient } from "pg";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { requireEnvironment } from "../support/environment.js";

const databaseUrl = requireEnvironment("TEST_DATABASE_URL");

// Consumers detect lost events by version gaps, so every version change must
// be a single step that commits together with its outbox event.
describe("auction version PostgreSQL schema", () => {
  let database: Pool;
  let ownerId: string;
  let auctionId: string;

  // Appends an event for the auction's current version, as a writer does
  // right after bumping it.
  async function appendEvent(client: PoolClient): Promise<void> {
    await client.query(
      `INSERT INTO outbox_events (
         event_id, event_type, occurred_at, auction_id, auction_version, payload
       )
       SELECT event.id, 'Test.v1', event.at, auctions.id, auctions.version,
              jsonb_build_object(
                'eventId', event.id::text,
                'eventType', 'Test.v1',
                'occurredAt',
                  to_char(event.at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                'auctionId', auctions.id::text,
                'auctionVersion', auctions.version::text
              )
       FROM auctions
       CROSS JOIN (SELECT gen_random_uuid() AS id, clock_timestamp() AS at) AS event
       WHERE auctions.id = $1`,
      [auctionId],
    );
  }

  async function inTransaction(work: (client: PoolClient) => Promise<void>): Promise<void> {
    const client = await database.connect();
    try {
      await client.query("BEGIN");
      await work(client);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async function currentVersion(): Promise<string> {
    const result = await database.query<{ version: string }>(
      "SELECT version::text AS version FROM auctions WHERE id = $1",
      [auctionId],
    );
    return result.rows[0]!.version;
  }

  beforeAll(() => {
    database = new Pool({ connectionString: databaseUrl });
  });

  beforeEach(async () => {
    const ownerResult = await database.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'Version Owner', '$argon2id$test')
       RETURNING id::text`,
      [`auction-version-owner-${randomUUID()}@example.com`],
    );
    ownerId = ownerResult.rows[0]!.id;

    const auctionResult = await database.query<{ id: string }>(
      `INSERT INTO auctions (
         owner_id, title, description, currency_code, starts_at, ends_at,
         starting_price_minor, minimum_bid_increment_minor
       )
       VALUES ($1, 'Version lot', 'An auction version fixture.', 'USD',
               now(), now() + interval '1 hour', 1000, 100)
       RETURNING id::text`,
      [ownerId],
    );
    auctionId = auctionResult.rows[0]!.id;
  });

  afterEach(async () => {
    await database.query("DELETE FROM outbox_events WHERE auction_id = $1", [auctionId]);
    await database.query("DELETE FROM auctions WHERE id = $1", [auctionId]);
    await database.query("DELETE FROM users WHERE id = $1", [ownerId]);
  });

  afterAll(async () => {
    await database.end();
  });

  it("commits a single-step version bump together with its event", async () => {
    await inTransaction(async (client) => {
      await client.query("UPDATE auctions SET version = version + 1 WHERE id = $1", [auctionId]);
      await appendEvent(client);
    });

    expect(await currentVersion()).toBe("2");
  });

  it("rejects at commit a version bump without an event, leaving the version unchanged", async () => {
    await expect(
      inTransaction(async (client) => {
        await client.query("UPDATE auctions SET version = version + 1 WHERE id = $1", [auctionId]);
      }),
    ).rejects.toMatchObject({ code: "23514", constraint: "auctions_version_outbox_event" });

    expect(await currentVersion()).toBe("1");
  });

  it("requires an event for every bump when one transaction bumps twice", async () => {
    await expect(
      inTransaction(async (client) => {
        await client.query("UPDATE auctions SET version = version + 1 WHERE id = $1", [auctionId]);
        await client.query("UPDATE auctions SET version = version + 1 WHERE id = $1", [auctionId]);
        await appendEvent(client);
      }),
    ).rejects.toMatchObject({ code: "23514", constraint: "auctions_version_outbox_event" });
    expect(await currentVersion()).toBe("1");

    await inTransaction(async (client) => {
      for (let step = 0; step < 2; step += 1) {
        await client.query("UPDATE auctions SET version = version + 1 WHERE id = $1", [auctionId]);
        await appendEvent(client);
      }
    });
    expect(await currentVersion()).toBe("3");
  });

  it("checks the event before commit when constraints are set immediate", async () => {
    await expect(
      inTransaction(async (client) => {
        await client.query("UPDATE auctions SET version = version + 1 WHERE id = $1", [auctionId]);
        await client.query("SET CONSTRAINTS auctions_version_outbox_event IMMEDIATE");
      }),
    ).rejects.toMatchObject({ code: "23514", constraint: "auctions_version_outbox_event" });
  });

  it.each([
    ["skips a version", "version + 2"],
    ["moves backwards", "version - 1"],
    ["jumps to an arbitrary version", "42"],
  ])("rejects an update that %s immediately", async (_case, expression) => {
    await expect(
      database.query(`UPDATE auctions SET version = ${expression} WHERE id = $1`, [auctionId]),
    ).rejects.toMatchObject({ code: "23514", constraint: "auctions_version_single_step" });

    expect(await currentVersion()).toBe("1");
  });

  it("allows updates that leave the version unchanged without an event", async () => {
    await database.query("UPDATE auctions SET version = version, title = 'Renamed' WHERE id = $1", [
      auctionId,
    ]);
    await database.query("UPDATE auctions SET updated_at = clock_timestamp() WHERE id = $1", [
      auctionId,
    ]);

    expect(await currentVersion()).toBe("1");
  });
});
