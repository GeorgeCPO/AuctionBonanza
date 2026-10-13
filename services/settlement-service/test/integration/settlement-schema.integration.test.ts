import { randomUUID } from "node:crypto";

import type { AuctionClosedV1 } from "@auction-bonanza/contracts";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { AuctionFixtures } from "../support/auction-fixtures.js";
import { requireEnvironment } from "../support/environment.js";
import { createIsolatedDatabase } from "../support/isolated-database.js";
import type { IsolatedDatabase } from "../support/isolated-database.types.js";

const adminDatabaseUrl = requireEnvironment("TEST_DATABASE_URL");

interface Refusal {
  code: string;
  constraint: string | undefined;
}

// The rules the database enforces on orders, whatever wrote the row. The
// consumer is never involved: these tests insert with SQL, as a buggy consumer
// or a person with psql could.
describe("settlement schema", () => {
  let isolated: IsolatedDatabase;
  let pool: Pool;
  let fixtures: AuctionFixtures;
  let ownerId: string;
  let winnerId: string;
  let loserId: string;

  beforeAll(async () => {
    isolated = await createIsolatedDatabase(adminDatabaseUrl);
    pool = new Pool({ connectionString: isolated.url, max: 4 });
    fixtures = new AuctionFixtures(pool);
  });

  beforeEach(async () => {
    await fixtures.reset();
    ownerId = await fixtures.user();
    winnerId = await fixtures.user();
    loserId = await fixtures.user();
  });

  afterAll(async () => {
    await pool.end();
    await isolated.drop();
  });

  // A closed auction where the loser bid first and the winner outbid them.
  async function auctionWithTwoBids(): Promise<{
    event: AuctionClosedV1;
    losingBidId: string;
    winningBidId: string;
  }> {
    const auctionId = await fixtures.openAuction(ownerId);
    const losingBidId = await fixtures.placeBid(auctionId, loserId, "1500");
    const winningBidId = await fixtures.placeBid(auctionId, winnerId, "1700");
    return { event: await fixtures.close(auctionId), losingBidId, winningBidId };
  }

  // Records an event as handled for an auction. An order's source event must be
  // the one recorded for the order's own auction.
  async function recordEvent(auctionId: string, eventId: string = randomUUID()): Promise<string> {
    await pool.query(
      `INSERT INTO settlement_processed_events (event_id, event_type, auction_id, auction_version)
       VALUES ($1, 'AuctionClosed.v1', $2, 2)`,
      [eventId, auctionId],
    );
    return eventId;
  }

  interface OrderValues {
    auctionId: string;
    winningBidId: string;
    buyerId: string;
    amountMinor: string;
    currencyCode: string;
    sourceEventId: string;
  }

  // Runs on the pool, or on a client when the insert belongs to a transaction.
  async function insertOrder(
    values: OrderValues,
    db: { query(text: string, values: string[]): Promise<unknown> } = pool,
  ): Promise<void> {
    await db.query(
      `INSERT INTO orders (
         auction_id, winning_bid_id, buyer_id, amount_minor, currency_code, source_event_id
       )
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        values.auctionId,
        values.winningBidId,
        values.buyerId,
        values.amountMinor,
        values.currencyCode,
        values.sourceEventId,
      ],
    );
  }

  // The order the closed auction's winner is owed.
  async function correctOrder(
    event: AuctionClosedV1,
    overrides: Partial<OrderValues> = {},
  ): Promise<OrderValues> {
    return {
      auctionId: event.auctionId,
      winningBidId: event.winningBidId!,
      buyerId: event.winnerId!,
      amountMinor: event.finalAmountMinor!,
      currencyCode: event.currencyCode,
      sourceEventId: await recordEvent(event.auctionId),
      ...overrides,
    };
  }

  async function refusalOf(action: Promise<unknown>): Promise<Refusal> {
    try {
      await action;
    } catch (error) {
      const { code, constraint } = error as { code: string; constraint?: string };
      return { code, constraint };
    }
    throw new Error("the database accepted a row it should have refused");
  }

  describe("an order", () => {
    it("is accepted when it names the closed auction's winner, bidder, price and currency", async () => {
      const { event } = await fixtures.soldAuction(ownerId, winnerId, "1500");

      await insertOrder(await correctOrder(event));

      expect(await fixtures.orders()).toEqual([
        expect.objectContaining({
          auctionId: event.auctionId,
          winningBidId: event.winningBidId,
          buyerId: winnerId,
          amountMinor: "1500",
          currencyCode: "EUR",
        }),
      ]);
    });

    it("is refused for an auction that is still open", async () => {
      const auctionId = await fixtures.openAuction(ownerId);
      const bidId = await fixtures.placeBid(auctionId, winnerId, "1500");

      const refusal = await refusalOf(
        insertOrder({
          auctionId,
          winningBidId: bidId,
          buyerId: winnerId,
          amountMinor: "1500",
          currencyCode: "EUR",
          sourceEventId: await recordEvent(auctionId),
        }),
      );

      expect(refusal).toEqual({ code: "23514", constraint: "orders_auction_closed" });
    });

    it("is refused for an auction that does not exist", async () => {
      const { event } = await fixtures.soldAuction(ownerId, winnerId);

      const refusal = await refusalOf(
        insertOrder(await correctOrder(event, { auctionId: "999999" })),
      );

      expect(refusal).toEqual({ code: "23514", constraint: "orders_auction_closed" });
    });

    it("is refused for an auction that closed unsold", async () => {
      const unsold = await fixtures.unsoldAuction(ownerId);
      const { event: other } = await fixtures.soldAuction(ownerId, winnerId);

      const refusal = await refusalOf(
        insertOrder(await correctOrder(other, { auctionId: unsold.auctionId })),
      );

      expect(refusal.constraint).toBe("orders_winning_bid_is_auction_winner");
    });

    it("is refused when it names a bid that did not win", async () => {
      const { event, losingBidId } = await auctionWithTwoBids();

      const refusal = await refusalOf(
        insertOrder(
          await correctOrder(event, {
            winningBidId: losingBidId,
            buyerId: loserId,
            amountMinor: "1500",
          }),
        ),
      );

      expect(refusal).toEqual({
        code: "23514",
        constraint: "orders_winning_bid_is_auction_winner",
      });
    });

    it("is refused when it names a bid of another auction", async () => {
      const { event } = await fixtures.soldAuction(ownerId, winnerId, "1500");
      const { event: other } = await fixtures.soldAuction(ownerId, loserId, "1900");

      const refusal = await refusalOf(
        insertOrder(await correctOrder(event, { winningBidId: other.winningBidId! })),
      );

      expect(refusal.constraint).toBe("orders_winning_bid_is_auction_winner");
    });

    it("is refused when it names the winning bid but the wrong buyer", async () => {
      const { event } = await fixtures.soldAuction(ownerId, winnerId);

      const refusal = await refusalOf(insertOrder(await correctOrder(event, { buyerId: loserId })));

      expect(refusal).toEqual({ code: "23514", constraint: "orders_terms_match_winning_bid" });
    });

    it("is refused when it names the winning bid but the wrong price", async () => {
      const { event } = await fixtures.soldAuction(ownerId, winnerId, "1500");

      const refusal = await refusalOf(insertOrder(await correctOrder(event, { amountMinor: "1" })));

      expect(refusal.constraint).toBe("orders_terms_match_winning_bid");
    });

    it("is refused when it names the winning bid but the wrong currency", async () => {
      const { event } = await fixtures.soldAuction(ownerId, winnerId);

      const refusal = await refusalOf(
        insertOrder(await correctOrder(event, { currencyCode: "USD" })),
      );

      expect(refusal.constraint).toBe("orders_terms_match_winning_bid");
    });

    it("is refused for a second order of the same auction", async () => {
      const { event } = await fixtures.soldAuction(ownerId, winnerId);
      await insertOrder(await correctOrder(event));

      const refusal = await refusalOf(insertOrder(await correctOrder(event)));

      expect(refusal).toEqual({ code: "23505", constraint: "orders_auction_id_key" });
      expect(await fixtures.orders()).toHaveLength(1);
    });

    it("is refused when no processed event produced it", async () => {
      const { event } = await fixtures.soldAuction(ownerId, winnerId);

      const refusal = await refusalOf(
        insertOrder(await correctOrder(event, { sourceEventId: randomUUID() })),
      );

      expect(refusal).toEqual({ code: "23503", constraint: "orders_source_event_id_fkey" });
    });

    it("is refused when its source event was recorded for another auction", async () => {
      const first = await fixtures.soldAuction(ownerId, winnerId);
      const second = await fixtures.soldAuction(ownerId, winnerId);
      const otherAuctionsEvent = await recordEvent(second.event.auctionId);

      const refusal = await refusalOf(
        insertOrder(await correctOrder(first.event, { sourceEventId: otherAuctionsEvent })),
      );

      expect(refusal).toEqual({ code: "23503", constraint: "orders_source_event_id_fkey" });
    });

    it("cannot be made by two orders sharing one event", async () => {
      const first = await fixtures.soldAuction(ownerId, winnerId);
      const second = await fixtures.soldAuction(ownerId, winnerId);
      const sourceEventId = await recordEvent(first.event.auctionId);
      await insertOrder(await correctOrder(first.event, { sourceEventId }));

      const refusal = await refusalOf(
        insertOrder(await correctOrder(second.event, { sourceEventId })),
      );

      expect(refusal).toEqual({ code: "23505", constraint: "orders_source_event_id_key" });
    });
  });

  describe("the foreign key to the winning bid", () => {
    it("is an independent second layer behind the trigger", async () => {
      const { event } = await fixtures.soldAuction(ownerId, winnerId);
      const { event: other } = await fixtures.soldAuction(ownerId, winnerId);
      const values = await correctOrder(event, { winningBidId: other.winningBidId! });

      // With the trigger switched off, inside a transaction that is rolled
      // back, the key alone must still refuse another auction's bid.
      const client = await pool.connect();
      let refusal: Refusal | undefined;
      try {
        await client.query("BEGIN");
        await client.query("ALTER TABLE orders DISABLE TRIGGER orders_winning_bid");
        refusal = await refusalOf(insertOrder(values, client));
      } finally {
        await client.query("ROLLBACK");
        client.release();
      }

      expect(refusal).toEqual({ code: "23503", constraint: "orders_winning_bid_id_fkey" });
    });
  });

  describe("an existing order", () => {
    async function existingOrder(): Promise<AuctionClosedV1> {
      const { event } = await fixtures.soldAuction(ownerId, winnerId, "1500");
      await insertOrder(await correctOrder(event));
      return event;
    }

    it.each([
      ["its price", "amount_minor = 1600"],
      ["its currency", "currency_code = 'USD'"],
      ["its buyer", "buyer_id = (SELECT id FROM users WHERE id <> orders.buyer_id LIMIT 1)"],
      ["its source event", "source_event_id = gen_random_uuid()"],
      ["its creation time", "created_at = created_at + interval '1 day'"],
    ])("cannot have %s changed", async (_description, assignment) => {
      await existingOrder();

      const refusal = await refusalOf(pool.query(`UPDATE orders SET ${assignment}`));

      expect(refusal).toEqual({ code: "23514", constraint: "orders_facts_immutable" });
    });

    it("can be rewritten with the values it already has", async () => {
      await existingOrder();

      await pool.query("UPDATE orders SET amount_minor = amount_minor");

      expect(await fixtures.orders()).toHaveLength(1);
    });
  });

  describe("the record of processed events", () => {
    it("holds an event once", async () => {
      const eventId = await recordEvent("1");

      // Under another auction the composite key does not conflict, so it is the
      // primary key alone that refuses the repeat.
      const refusal = await refusalOf(recordEvent("2", eventId));

      expect(refusal).toEqual({ code: "23505", constraint: "settlement_processed_events_pkey" });
    });

    it("refuses an auction version below 1", async () => {
      const refusal = await refusalOf(
        pool.query(
          `INSERT INTO settlement_processed_events (event_id, event_type, auction_id, auction_version)
           VALUES (gen_random_uuid(), 'AuctionClosed.v1', 1, 0)`,
        ),
      );

      expect(refusal).toEqual({
        code: "23514",
        constraint: "settlement_processed_events_auction_version_check",
      });
    });
  });
});
