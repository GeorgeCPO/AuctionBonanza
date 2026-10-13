import { randomUUID } from "node:crypto";

import { parseAuctionClosedV1, type AuctionClosedV1 } from "@auction-bonanza/contracts";
import type { Pool, PoolClient } from "pg";

import type { OrderRow } from "./auction-fixtures.types.js";

// Builds the rows settlement reads, with plain SQL. The settlement tests may
// not use auction-service's code, so this does by hand what the bid and
// finalization transactions do, including the version step and outbox event
// the database requires with every close.
export class AuctionFixtures {
  constructor(private readonly pool: Pool) {}

  async user(): Promise<string> {
    const result = await this.pool.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'Settlement Tester', '$argon2id$test')
       RETURNING id::text`,
      [`settlement-${randomUUID()}@example.com`],
    );
    return result.rows[0]!.id;
  }

  // Empties every table a test writes, with identities restarted. The one place
  // that lists them, so a new settlement table is added here and nowhere else.
  async reset(): Promise<void> {
    await this.pool.query(
      "TRUNCATE orders, settlement_processed_events, outbox_events, bids, auctions, users RESTART IDENTITY CASCADE",
    );
  }

  async openAuction(ownerId: string): Promise<string> {
    const result = await this.pool.query<{ id: string }>(
      `INSERT INTO auctions (
         owner_id, title, description, currency_code, starts_at, ends_at,
         starting_price_minor, minimum_bid_increment_minor
       )
       VALUES ($1, 'Settlement lot', 'A settlement fixture.', 'EUR',
               now() - interval '1 minute', now() + interval '1 hour', 1000, 100)
       RETURNING id::text`,
      [ownerId],
    );
    return result.rows[0]!.id;
  }

  // Records an accepted bid and makes it the leading one. Returns the bid id.
  async placeBid(auctionId: string, bidderId: string, amountMinor: string): Promise<string> {
    return this.inTransaction(async (client) => {
      const bid = await client.query<{ id: string }>(
        `INSERT INTO bids (auction_id, bidder_id, amount_minor, idempotency_key, created_at)
         VALUES ($1, $2, $3, $4, clock_timestamp())
         RETURNING id::text`,
        [auctionId, bidderId, amountMinor, randomUUID()],
      );
      const bidId = bid.rows[0]!.id;
      await client.query("UPDATE auctions SET leading_bid_id = $2 WHERE id = $1", [
        auctionId,
        bidId,
      ]);
      return bidId;
    });
  }

  // Closes the auction as the finalizer does, in one transaction: moves its
  // deadline into the past, sets the outcome and steps the version, and writes
  // the AuctionClosed.v1 outbox event the version trigger requires. Returns
  // the event, as the outbox publisher would send it.
  async close(auctionId: string): Promise<AuctionClosedV1> {
    return this.inTransaction(async (client) => {
      await client.query(
        `UPDATE auctions
         SET starts_at = clock_timestamp() - interval '2 hours',
             ends_at = clock_timestamp() - interval '1 second'
         WHERE id = $1`,
        [auctionId],
      );
      const closed = await client.query<Record<string, string | null>>(
        `UPDATE auctions
         SET closed_at = clock_timestamp(),
             updated_at = clock_timestamp(),
             winning_bid_id = leading_bid_id,
             version = version + 1
         WHERE id = $1
         RETURNING version::text AS version, currency_code, winning_bid_id::text AS winning_bid_id,
                   to_char(closed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS occurred_at`,
        [auctionId],
      );
      const auction = closed.rows[0]!;
      const winningBidId = auction.winning_bid_id ?? null;

      let winnerId: string | null = null;
      let finalAmountMinor: string | null = null;
      if (winningBidId !== null) {
        const bid = await client.query<{ bidder_id: string; amount_minor: string }>(
          "SELECT bidder_id::text, amount_minor::text FROM bids WHERE id = $1",
          [winningBidId],
        );
        winnerId = bid.rows[0]!.bidder_id;
        finalAmountMinor = bid.rows[0]!.amount_minor;
      }

      const event = parseAuctionClosedV1({
        eventId: randomUUID(),
        eventType: "AuctionClosed.v1",
        occurredAt: auction.occurred_at,
        auctionId,
        auctionVersion: auction.version,
        currencyCode: auction.currency_code,
        winningBidId,
        winnerId,
        finalAmountMinor,
      });
      await client.query(
        `INSERT INTO outbox_events (
           event_id, event_type, occurred_at, auction_id, auction_version, payload
         )
         VALUES ($1, $2, $3::timestamptz, $4, $5, $6::jsonb)`,
        [
          event.eventId,
          event.eventType,
          event.occurredAt,
          event.auctionId,
          event.auctionVersion,
          JSON.stringify(event),
        ],
      );
      return event;
    });
  }

  // A closed auction with a winner: one bid, then the close.
  async soldAuction(
    ownerId: string,
    bidderId: string,
    amountMinor = "1500",
  ): Promise<{ event: AuctionClosedV1; bidId: string }> {
    const auctionId = await this.openAuction(ownerId);
    const bidId = await this.placeBid(auctionId, bidderId, amountMinor);
    return { event: await this.close(auctionId), bidId };
  }

  async unsoldAuction(ownerId: string): Promise<AuctionClosedV1> {
    return this.close(await this.openAuction(ownerId));
  }

  async orders(): Promise<OrderRow[]> {
    const result = await this.pool.query<Record<string, string>>(
      `SELECT id::text, auction_id::text, winning_bid_id::text, buyer_id::text,
              amount_minor::text, currency_code, source_event_id::text
       FROM orders ORDER BY id`,
    );
    return result.rows.map((row) => ({
      id: row.id!,
      auctionId: row.auction_id!,
      winningBidId: row.winning_bid_id!,
      buyerId: row.buyer_id!,
      amountMinor: row.amount_minor!,
      currencyCode: row.currency_code!,
      sourceEventId: row.source_event_id!,
    }));
  }

  async processedEventIds(): Promise<string[]> {
    const result = await this.pool.query<{ event_id: string }>(
      "SELECT event_id::text FROM settlement_processed_events ORDER BY event_id",
    );
    return result.rows.map((row) => row.event_id);
  }

  // Closed auctions that have a winner and no order: the operator's check for
  // an event settlement never consumed (docs/SETTLEMENT.md).
  async unsettledAuctionIds(): Promise<string[]> {
    const result = await this.pool.query<{ id: string }>(
      `SELECT auctions.id::text
       FROM auctions
       LEFT JOIN orders ON orders.auction_id = auctions.id
       WHERE auctions.winning_bid_id IS NOT NULL AND orders.id IS NULL
       ORDER BY auctions.closed_at, auctions.id`,
    );
    return result.rows.map((row) => row.id);
  }

  private async inTransaction<Result>(
    work: (client: PoolClient) => Promise<Result>,
  ): Promise<Result> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await work(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}
