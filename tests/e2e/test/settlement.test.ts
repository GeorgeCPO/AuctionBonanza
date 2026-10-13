import { randomInt, randomUUID } from "node:crypto";

import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { requireEnvironment } from "./support/environment.js";
import { createIsolatedDatabase } from "./support/isolated-database.js";
import type { IsolatedDatabase } from "./support/isolated-database.types.js";
import { TestTopic } from "./support/kafka.js";
import {
  auctionServiceRoot,
  compileService,
  freePort,
  settlementServiceRoot,
  startServiceProcess,
} from "./support/services.js";
import type { CompiledService, ServiceProcess } from "./support/services.types.js";
import { startTestIssuer } from "./support/test-issuer.js";
import type { TestIssuer } from "./support/test-issuer.types.js";
import { waitFor } from "./support/wait-for.js";

const databaseUrl = requireEnvironment("TEST_DATABASE_URL");
const kafkaBrokers = requireEnvironment("TEST_KAFKA_BROKERS");

// The path of a closed auction to its order, with every hop real and every
// service a black box: each is compiled from source and run as its own
// process, and the test talks to them only over HTTP, PostgreSQL, and the
// infrastructure from the root docker-compose.yml.
//
//   POST /bids -> auction-service API
//   the deadline passes -> finalizer -> outbox -> outbox publisher -> Kafka
//     -> settlement worker -> orders
//
// identity-service is stood in for by a test issuer that signs the access
// tokens auction-service verifies.
describe("settlement, end to end", { timeout: 180_000 }, () => {
  let auctionService: CompiledService | undefined;
  let settlementService: CompiledService | undefined;
  let database: IsolatedDatabase | undefined;
  let pool: Pool | undefined;
  let issuer: TestIssuer | undefined;
  let events: TestTopic | undefined;
  let deadLetters: TestTopic | undefined;
  const processes: ServiceProcess[] = [];
  let settlement: ServiceProcess;
  let apiUrl: string;

  beforeAll(async () => {
    events = new TestTopic(kafkaBrokers);
    deadLetters = new TestTopic(kafkaBrokers);
    // Independent of each other; the two compiles are the long pole.
    [auctionService, settlementService, database, issuer] = await Promise.all([
      compileService(auctionServiceRoot, "e2e"),
      compileService(settlementServiceRoot, "e2e"),
      createIsolatedDatabase(databaseUrl, BigInt(randomInt(1_000_000_000, 2 ** 47))),
      startTestIssuer(),
      events.create(3),
      deadLetters.create(1),
    ]);
    pool = new Pool({ connectionString: database.url, max: 2 });
    // DROP DATABASE ... WITH (FORCE) in afterAll can terminate a connection
    // that pool.end() asked to close but the server has not processed yet.
    pool.on("error", () => {});

    const api = start(auctionService, "index.js", {
      HOST: "127.0.0.1",
      PORT: String(await freePort()),
      DATABASE_URL: database.url,
      AUTH_ISSUER: issuer.issuer,
      AUTH_AUDIENCE: issuer.audience,
      AUTH_JWKS_URL: issuer.jwksUrl,
    });
    apiUrl = /http:\/\/\S+/.exec(String((await api.waitForLog("Server listening at")).msg))![0];
    start(auctionService, "outbox-publisher.main.js", {
      DATABASE_URL: database.url,
      KAFKA_BROKERS: kafkaBrokers,
      AUCTION_EVENTS_TOPIC: events.name,
      OUTBOX_POLL_INTERVAL_MS: "100",
    });
    start(auctionService, "finalizer.main.js", {
      DATABASE_URL: database.url,
      FINALIZER_POLL_INTERVAL_MS: "100",
    });
    settlement = start(settlementService, "settlement.main.js", {
      DATABASE_URL: database.url,
      KAFKA_BROKERS: kafkaBrokers,
      KAFKA_GROUP_ID: events.groupId,
      AUCTION_EVENTS_TOPIC: events.name,
      SETTLEMENT_DEAD_LETTER_TOPIC: deadLetters.name,
    });
    await settlement.waitForLog("settlement started", 60_000);
  });

  afterAll(async () => {
    // The settlement worker must leave its group before the group can be
    // deleted.
    await Promise.all(processes.map((child) => child.stop()));
    await Promise.all([
      events?.remove(),
      deadLetters?.remove(),
      issuer?.stop(),
      auctionService?.remove(),
      settlementService?.remove(),
      (async () => {
        await pool?.end();
        await database?.drop();
      })(),
    ]);
  });

  function start(
    service: CompiledService,
    entryFile: string,
    environment: Record<string, string>,
  ): ServiceProcess {
    const child = startServiceProcess(service, entryFile, environment);
    processes.push(child);
    return child;
  }

  async function insertUser(): Promise<string> {
    const result = await pool!.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'End to end', '$argon2id$test')
       RETURNING id::text`,
      [`e2e-${randomUUID()}@example.com`],
    );
    return result.rows[0]!.id;
  }

  async function post(path: string, userId: string, body: unknown): Promise<Response> {
    return fetch(`${apiUrl}${path}`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${issuer!.accessToken(userId)}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    });
  }

  async function createAuction(ownerId: string): Promise<string> {
    const created = await post("/v1/auctions", ownerId, {
      title: "Settlement lot",
      description: "Closed, then settled.",
      currencyCode: "USD",
      endsAt: new Date(Date.now() + 3_600_000).toISOString(),
      startingPriceMinor: "1000",
      minimumBidIncrementMinor: "100",
    });
    expect(created.status).toBe(201);
    return ((await created.json()) as { id: string }).id;
  }

  async function placeBid(
    auctionId: string,
    bidderId: string,
    amountMinor: string,
  ): Promise<string> {
    const placed = await post(`/v1/auctions/${auctionId}/bids`, bidderId, {
      amountMinor,
      idempotencyKey: randomUUID(),
    });
    expect(placed.status).toBe(201);
    return ((await placed.json()) as { bid: { id: string } }).bid.id;
  }

  // Time cannot be waited for (an auction lasts at least a minute), so the
  // deadline is moved into the past. Everything after that is the finalizer,
  // the outbox publisher and the settlement worker acting on their own.
  async function passDeadline(auctionId: string): Promise<void> {
    await pool!.query(
      `UPDATE auctions
       SET starts_at = clock_timestamp() - interval '2 hours',
           ends_at = clock_timestamp() - interval '1 second'
       WHERE id = $1`,
      [auctionId],
    );
  }

  async function handledEvents(auctionId: string): Promise<number> {
    const result = await pool!.query(
      "SELECT 1 FROM settlement_processed_events WHERE auction_id = $1",
      [auctionId],
    );
    return result.rows.length;
  }

  async function ordersOf(auctionId: string): Promise<Record<string, string>[]> {
    const result = await pool!.query<Record<string, string>>(
      `SELECT id::text, winning_bid_id::text, buyer_id::text, amount_minor::text, currency_code
       FROM orders WHERE auction_id = $1`,
      [auctionId],
    );
    return result.rows;
  }

  it("turns a closed auction into one order for its winner", async () => {
    const ownerId = await insertUser();
    const bidderId = await insertUser();
    const winnerId = await insertUser();
    const auctionId = await createAuction(ownerId);
    await placeBid(auctionId, bidderId, "1000");
    const winningBidId = await placeBid(auctionId, winnerId, "1100");

    await passDeadline(auctionId);
    await waitFor(
      async () => (await ordersOf(auctionId)).length > 0,
      () => `an order for auction ${auctionId}\n${settlement.stdout()}`,
      60_000,
    );

    expect(await ordersOf(auctionId)).toEqual([
      {
        id: expect.any(String),
        winning_bid_id: winningBidId,
        buyer_id: winnerId,
        amount_minor: "1100",
        currency_code: "USD",
      },
    ]);
    const response = await fetch(`${apiUrl}/v1/auctions/${auctionId}`);
    expect(await response.json()).toMatchObject({
      phase: "closed",
      winningBidId,
    });
    expect(await handledEvents(auctionId)).toBe(1);
  });

  it("settles an auction that closed unsold into no order", async () => {
    const ownerId = await insertUser();
    const auctionId = await createAuction(ownerId);

    await passDeadline(auctionId);
    await waitFor(
      async () => (await handledEvents(auctionId)) > 0,
      () => `the unsold close of auction ${auctionId} handled\n${settlement.stdout()}`,
      60_000,
    );

    expect(await ordersOf(auctionId)).toEqual([]);
    const response = await fetch(`${apiUrl}/v1/auctions/${auctionId}`);
    expect(await response.json()).toMatchObject({ phase: "closed", winningBidId: null });
  });
});
