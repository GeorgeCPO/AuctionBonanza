import type { AuctionClosedV1 } from "@auction-bonanza/contracts";

import { classifySettlementError } from "./settlement.errors.js";
import type { SettlementOutcome, SettlementRepository } from "./settlement.repository.types.js";
import { rollbackQuietly, toError, type Database, type DatabaseClient } from "./shared/database.js";

// Settles AuctionClosed.v1 events into orders. See docs/SETTLEMENT.md.
//
// One transaction per event:
//
//   1. Record the event in settlement_processed_events. If it is already
//      there, an earlier delivery handled it: commit nothing and report a
//      duplicate. The primary key is what makes this safe under concurrency:
//      two deliveries racing on one event serialize on the same index entry,
//      and the second one sees the first's committed row.
//   2. If the auction was sold, insert its order. The orders table and its
//      trigger enforce the rest: one order per auction, and an order that
//      names exactly the auction's recorded winner, bidder, amount and
//      currency.
//   3. COMMIT.
//
// The record and the order commit together or not at all, so there is no
// state in which an event counts as handled but its order is missing, or the
// other way round. That is the whole acknowledgement story: the caller
// acknowledges the Kafka message only after this method returns.
export class PostgresSettlementRepository implements SettlementRepository {
  constructor(private readonly database: Database) {}

  async settle(event: AuctionClosedV1): Promise<SettlementOutcome> {
    const client = await this.database.connect();
    let transactionError: unknown;

    try {
      await client.query("BEGIN");
      const outcome = await this.apply(client, event);
      await client.query("COMMIT");
      return outcome;
    } catch (error) {
      transactionError = error;
      await rollbackQuietly(client);
      throw classifySettlementError(error);
    } finally {
      // A client in an unknown transaction state must not return to the pool.
      client.release(transactionError === undefined ? undefined : toError(transactionError));
    }
  }

  private async apply(client: DatabaseClient, event: AuctionClosedV1): Promise<SettlementOutcome> {
    const recorded = await client.query(
      `INSERT INTO settlement_processed_events (event_id, event_type, auction_id, auction_version)
       VALUES ($1::uuid, $2, $3::bigint, $4::bigint)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [event.eventId, event.eventType, event.auctionId, event.auctionVersion],
    );
    if (recorded.rows.length === 0) {
      return { kind: "duplicate" };
    }

    // The contract allows exactly two shapes: all three outcome fields set, or
    // none. Checking all three also narrows their types.
    const { winningBidId, winnerId, finalAmountMinor } = event;
    if (winningBidId === null || winnerId === null || finalAmountMinor === null) {
      return { kind: "unsold_recorded" };
    }

    const order = await client.query(
      `INSERT INTO orders (
         auction_id, winning_bid_id, buyer_id, amount_minor, currency_code, source_event_id
       )
       VALUES ($1::bigint, $2::bigint, $3::bigint, $4::bigint, $5, $6::uuid)
       RETURNING id::text AS id`,
      [
        event.auctionId,
        winningBidId,
        winnerId,
        finalAmountMinor,
        event.currencyCode,
        event.eventId,
      ],
    );
    const created = order.rows[0] as { id: string } | undefined;
    if (created === undefined) {
      throw new Error(`inserting the order for auction ${event.auctionId} returned no row`);
    }
    return { kind: "order_created", orderId: created.id };
  }
}
