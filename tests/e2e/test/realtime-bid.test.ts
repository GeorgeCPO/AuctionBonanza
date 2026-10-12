import { randomInt, randomUUID } from "node:crypto";

import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createIsolatedDatabase } from "./support/isolated-database.js";
import type { IsolatedDatabase } from "./support/isolated-database.types.js";
import { TestTopic } from "./support/kafka.js";
import {
  auctionServiceRoot,
  compileService,
  freePort,
  gatewayRoot,
  startServiceProcess,
} from "./support/services.js";
import type { CompiledService, ServiceProcess } from "./support/services.types.js";
import { SseTestClient } from "./support/sse-client.js";
import { startTestIssuer } from "./support/test-issuer.js";
import type { TestIssuer } from "./support/test-issuer.types.js";

const databaseUrl = process.env.TEST_DATABASE_URL;
const kafkaBrokers = process.env.TEST_KAFKA_BROKERS;
const valkeyNodes = process.env.TEST_VALKEY_NODES;

// The whole path of a bid, with every hop real and every service a black box:
// each is compiled from source and run as its own process, and the test
// talks to them only over HTTP, PostgreSQL (to create users), and the
// infrastructure from the root docker-compose.yml.
//
//   POST /bids -> auction-service API -> outbox -> outbox publisher -> Kafka
//     -> relay -> Valkey -> edge -> SSE client
//
// and of the close that follows it, from the finalizer to the same client.
//
// identity-service is the one service stood in for: a test issuer signs the
// access tokens auction-service verifies.
describe.skipIf(
  databaseUrl === undefined || kafkaBrokers === undefined || valkeyNodes === undefined,
)("a bid, end to end", { timeout: 180_000 }, () => {
  let auctionService: CompiledService | undefined;
  let gateway: CompiledService | undefined;
  let database: IsolatedDatabase | undefined;
  let pool: Pool | undefined;
  let issuer: TestIssuer | undefined;
  let topic: TestTopic | undefined;
  const processes: ServiceProcess[] = [];
  let apiUrl: string;
  let edgeUrl: string;

  beforeAll(async () => {
    [auctionService, gateway] = await Promise.all([
      compileService(auctionServiceRoot, "e2e"),
      compileService(gatewayRoot, "e2e"),
    ]);
    // Auction ids start high: the Valkey cluster, and so the auction
    // channels, are shared with the services' own tests.
    database = await createIsolatedDatabase(
      databaseUrl!,
      BigInt(randomInt(1_000_000_000, 2 ** 47)),
    );
    pool = new Pool({ connectionString: database.url, max: 2 });
    // DROP DATABASE ... WITH (FORCE) in afterAll can terminate a connection
    // that pool.end() asked to close but the server has not processed yet.
    // pg reports that as an idle-client error, which would otherwise be an
    // uncaught exception that fails the run.
    pool.on("error", () => {});
    issuer = await startTestIssuer();
    topic = new TestTopic(kafkaBrokers!);
    await topic.create(3);

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
      KAFKA_BROKERS: kafkaBrokers!,
      AUCTION_EVENTS_TOPIC: topic.name,
      OUTBOX_POLL_INTERVAL_MS: "100",
    });
    start(auctionService, "finalizer.main.js", {
      DATABASE_URL: database.url,
      FINALIZER_POLL_INTERVAL_MS: "100",
    });
    const relay = start(gateway, "relay.main.js", {
      KAFKA_BROKERS: kafkaBrokers!,
      KAFKA_GROUP_ID: topic.groupId,
      AUCTION_EVENTS_TOPIC: topic.name,
      VALKEY_NODES: valkeyNodes!,
    });
    await relay.waitForLog("relay started");
    const edge = start(gateway, "edge.main.js", {
      HOST: "127.0.0.1",
      PORT: "0",
      VALKEY_NODES: valkeyNodes!,
      AUCTION_SERVICE_URL: apiUrl,
    });
    edgeUrl = `http://127.0.0.1:${String((await edge.waitForLog("edge started")).port)}`;
  });

  afterAll(async () => {
    // The relay must leave its group before the group can be deleted.
    await Promise.all(processes.map((child) => child.stop()));
    await topic?.remove();
    await pool?.end();
    await database?.drop();
    await issuer?.stop();
    await auctionService?.remove();
    await gateway?.remove();
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
      title: "Realtime lot",
      description: "Watched over SSE.",
      currencyCode: "USD",
      endsAt: new Date(Date.now() + 3_600_000).toISOString(),
      startingPriceMinor: "1000",
      minimumBidIncrementMinor: "100",
    });
    expect(created.status).toBe(201);
    return ((await created.json()) as { id: string }).id;
  }

  it("streams a bid accepted over HTTP to an SSE client", async () => {
    const ownerId = await insertUser();
    const bidderId = await insertUser();
    const auctionId = await createAuction(ownerId);

    const client = await SseTestClient.open(`${edgeUrl}/v1/stream?auctions=${auctionId}`);
    try {
      await client.waitForTimeline(auctionId, ["snapshot@1"]);
      expect(client.messages[0]?.data).toMatchObject({
        auctionId,
        phase: "open",
        currencyCode: "USD",
        leadingBid: null,
      });

      const placed = await post(`/v1/auctions/${auctionId}/bids`, bidderId, {
        amountMinor: "1000",
        idempotencyKey: randomUUID(),
      });
      expect(placed.status).toBe(201);
      const { bid } = (await placed.json()) as { bid: { id: string } };
      await client.waitForTimeline(auctionId, ["snapshot@1", "bid-accepted@2"]);

      expect(client.timeline(auctionId)).toEqual(["snapshot@1", "bid-accepted@2"]);
      expect(client.messages[1]).toEqual({
        event: "bid-accepted",
        data: {
          auctionId,
          auctionVersion: "2",
          bidId: bid.id,
          amountMinor: "1000",
          currencyCode: "USD",
          occurredAt: expect.any(String),
        },
      });
    } finally {
      client.close();
    }
  });

  it("closes an auction at its deadline and shows the winner to an SSE client", async () => {
    const ownerId = await insertUser();
    const bidderId = await insertUser();
    const otherBidderId = await insertUser();
    const auctionId = await createAuction(ownerId);
    const client = await SseTestClient.open(`${edgeUrl}/v1/stream?auctions=${auctionId}`);
    try {
      await client.waitForTimeline(auctionId, ["snapshot@1"]);
      await post(`/v1/auctions/${auctionId}/bids`, bidderId, {
        amountMinor: "1000",
        idempotencyKey: randomUUID(),
      });
      const winning = await post(`/v1/auctions/${auctionId}/bids`, otherBidderId, {
        amountMinor: "1100",
        idempotencyKey: randomUUID(),
      });
      const { bid } = (await winning.json()) as { bid: { id: string } };
      await client.waitForTimeline(auctionId, ["snapshot@1", "bid-accepted@2", "bid-accepted@3"]);

      // Time cannot be waited for (an auction lasts at least a minute), so
      // the deadline is moved into the past. Everything after that is the
      // finalizer process acting on its own.
      await pool!.query(
        `UPDATE auctions
         SET starts_at = clock_timestamp() - interval '2 hours',
             ends_at = clock_timestamp() - interval '1 second'
         WHERE id = $1`,
        [auctionId],
      );

      // The edge does not know AuctionClosed.v1, so it sees a version it
      // cannot apply and resyncs from auction-service: a snapshot at the new
      // version, showing the closed phase.
      await client.waitForTimeline(auctionId, [
        "snapshot@1",
        "bid-accepted@2",
        "bid-accepted@3",
        "snapshot@4",
      ]);
      expect(client.messages[3]?.data).toMatchObject({
        auctionId,
        auctionVersion: "4",
        phase: "closed",
        leadingBid: { bidId: bid.id, amountMinor: "1100" },
      });

      const response = await fetch(`${apiUrl}/v1/auctions/${auctionId}`);
      expect(await response.json()).toMatchObject({
        id: auctionId,
        phase: "closed",
        version: "4",
        winningBidId: bid.id,
      });
    } finally {
      client.close();
    }
  });
});
