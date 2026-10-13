import { randomInt } from "node:crypto";

import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { AuctionApi, ProcessGroup, startAuctionService } from "./support/auction-stack.js";
import { requireEnvironment } from "./support/environment.js";
import { createIsolatedDatabase } from "./support/isolated-database.js";
import type { IsolatedDatabase } from "./support/isolated-database.types.js";
import { TestTopic } from "./support/kafka.js";
import { auctionServiceRoot, compileService, gatewayRoot } from "./support/services.js";
import type { CompiledService } from "./support/services.types.js";
import { SseTestClient } from "./support/sse-client.js";
import { startTestIssuer } from "./support/test-issuer.js";
import type { TestIssuer } from "./support/test-issuer.types.js";

const databaseUrl = requireEnvironment("TEST_DATABASE_URL");
const kafkaBrokers = requireEnvironment("TEST_KAFKA_BROKERS");
const valkeyNodes = requireEnvironment("TEST_VALKEY_NODES");

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
describe("a bid, end to end", { timeout: 180_000 }, () => {
  let auctionService: CompiledService | undefined;
  let gateway: CompiledService | undefined;
  let database: IsolatedDatabase | undefined;
  let pool: Pool | undefined;
  let issuer: TestIssuer | undefined;
  let topic: TestTopic | undefined;
  const processes = new ProcessGroup();
  let api: AuctionApi;
  let apiUrl: string;
  let edgeUrl: string;

  beforeAll(async () => {
    [auctionService, gateway] = await Promise.all([
      compileService(auctionServiceRoot, "e2e"),
      compileService(gatewayRoot, "e2e"),
    ]);
    // Auction ids start high: the Valkey cluster, and so the auction
    // channels, are shared with the services' own tests.
    database = await createIsolatedDatabase(databaseUrl, BigInt(randomInt(1_000_000_000, 2 ** 47)));
    pool = new Pool({ connectionString: database.url, max: 2 });
    // DROP DATABASE ... WITH (FORCE) in afterAll can terminate a connection
    // that pool.end() asked to close but the server has not processed yet.
    // pg reports that as an idle-client error, which would otherwise be an
    // uncaught exception that fails the run.
    pool.on("error", () => {});
    issuer = await startTestIssuer();
    topic = new TestTopic(kafkaBrokers);
    await topic.create(3);

    apiUrl = await startAuctionService(processes, auctionService, {
      databaseUrl: database.url,
      issuer,
      kafkaBrokers,
      topic: topic.name,
    });
    api = new AuctionApi({ apiUrl, pool, issuer });
    const relay = processes.start(gateway, "relay.main.js", {
      KAFKA_BROKERS: kafkaBrokers,
      KAFKA_GROUP_ID: topic.groupId,
      AUCTION_EVENTS_TOPIC: topic.name,
      VALKEY_NODES: valkeyNodes,
    });
    await relay.waitForLog("relay started");
    const edge = processes.start(gateway, "edge.main.js", {
      HOST: "127.0.0.1",
      PORT: "0",
      VALKEY_NODES: valkeyNodes,
      AUCTION_SERVICE_URL: apiUrl,
    });
    edgeUrl = `http://127.0.0.1:${String((await edge.waitForLog("edge started")).port)}`;
  });

  afterAll(async () => {
    // The relay must leave its group before the group can be deleted.
    await processes.stopAll();
    await topic?.remove();
    await pool?.end();
    await database?.drop();
    await issuer?.stop();
    await auctionService?.remove();
    await gateway?.remove();
  });

  it("streams a bid accepted over HTTP to an SSE client", async () => {
    const ownerId = await api.insertUser();
    const bidderId = await api.insertUser();
    const auctionId = await api.createAuction(ownerId, "Realtime lot", "Watched over SSE.");

    const client = await SseTestClient.open(`${edgeUrl}/v1/stream?auctions=${auctionId}`);
    try {
      await client.waitForTimeline(auctionId, ["snapshot@1"]);
      expect(client.messages[0]?.data).toMatchObject({
        auctionId,
        phase: "open",
        currencyCode: "USD",
        leadingBid: null,
      });

      const bidId = await api.placeBid(auctionId, bidderId, "1000");
      await client.waitForTimeline(auctionId, ["snapshot@1", "bid-accepted@2"]);

      expect(client.timeline(auctionId)).toEqual(["snapshot@1", "bid-accepted@2"]);
      expect(client.messages[1]).toEqual({
        event: "bid-accepted",
        data: {
          auctionId,
          auctionVersion: "2",
          bidId,
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
    const ownerId = await api.insertUser();
    const bidderId = await api.insertUser();
    const otherBidderId = await api.insertUser();
    const auctionId = await api.createAuction(ownerId, "Realtime lot", "Watched over SSE.");
    const client = await SseTestClient.open(`${edgeUrl}/v1/stream?auctions=${auctionId}`);
    try {
      await client.waitForTimeline(auctionId, ["snapshot@1"]);
      await api.placeBid(auctionId, bidderId, "1000");
      const winningBidId = await api.placeBid(auctionId, otherBidderId, "1100");
      await client.waitForTimeline(auctionId, ["snapshot@1", "bid-accepted@2", "bid-accepted@3"]);

      // Everything after the deadline passes is the finalizer process acting
      // on its own.
      await api.passDeadline(auctionId);

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
        leadingBid: { bidId: winningBidId, amountMinor: "1100" },
      });

      const response = await fetch(`${apiUrl}/v1/auctions/${auctionId}`);
      expect(await response.json()).toMatchObject({
        id: auctionId,
        phase: "closed",
        version: "4",
        winningBidId,
      });
    } finally {
      client.close();
    }
  });
});
