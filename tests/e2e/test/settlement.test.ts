import { randomInt } from "node:crypto";

import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { AuctionApi, ProcessGroup, startAuctionService } from "./support/auction-stack.js";
import { requireEnvironment } from "./support/environment.js";
import { createIsolatedDatabase } from "./support/isolated-database.js";
import type { IsolatedDatabase } from "./support/isolated-database.types.js";
import { TestTopic } from "./support/kafka.js";
import { auctionServiceRoot, compileService, settlementServiceRoot } from "./support/services.js";
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
  const processes = new ProcessGroup();
  let settlement: ServiceProcess;
  let api: AuctionApi;
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

    apiUrl = await startAuctionService(processes, auctionService, {
      databaseUrl: database.url,
      issuer,
      kafkaBrokers,
      topic: events.name,
    });
    api = new AuctionApi({ apiUrl, pool, issuer });
    settlement = processes.start(settlementService, "settlement.main.js", {
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
    await processes.stopAll();
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
    const ownerId = await api.insertUser();
    const bidderId = await api.insertUser();
    const winnerId = await api.insertUser();
    const auctionId = await api.createAuction(ownerId, "Settlement lot", "Closed, then settled.");
    await api.placeBid(auctionId, bidderId, "1000");
    const winningBidId = await api.placeBid(auctionId, winnerId, "1100");

    await api.passDeadline(auctionId);
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
    const ownerId = await api.insertUser();
    const auctionId = await api.createAuction(ownerId, "Settlement lot", "Closed, then settled.");

    await api.passDeadline(auctionId);
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
