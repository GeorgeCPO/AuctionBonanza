import { randomUUID } from "node:crypto";

import { AUCTION_CLOSED_V1_EVENT_TYPE, parseAuctionClosedV1 } from "@auction-bonanza/contracts";

import { rollbackQuietly, toError, type Database } from "../database.js";
import { insertOutboxEvent } from "../outbox/outbox.store.js";
import type { FinalizationOutcome, FinalizationRepository } from "./finalization.types.js";

interface ClaimedAuctionRow extends Record<string, unknown> {
  id: string;
  currency_code: string;
  closable: boolean;
  leading_bid_id: string | null;
  leading_bidder_id: string | null;
  leading_amount_minor: string | null;
  observed_at_text: string;
}

// The due-auction predicate, shared by the cheap pre-check and the locking
// claim so they cannot drift apart.
const DUE_AUCTIONS = `
  FROM auctions
  WHERE cancelled_at IS NULL
    AND closed_at IS NULL
    AND ends_at <= clock_timestamp()
  ORDER BY ends_at, id
  LIMIT 1`;

// Closes auctions whose deadline has passed. Each call is one transaction for
// one auction, so a failure never holds or rolls back another auction's
// close, and row locks are held only for a few statements.
//
// The close, the version step and the AuctionClosed.v1 outbox event commit
// together or not at all. The triggers on `auctions` reject a close that
// skips any of them.
export class PostgresFinalizationRepository implements FinalizationRepository {
  constructor(private readonly database: Database) {}

  async finalizeNextDue(): Promise<FinalizationOutcome> {
    // Most polls find nothing due. Looking without a transaction or a lock
    // makes that one round trip instead of three; the claim below is what
    // decides, so a stale answer here only costs a wasted or delayed claim.
    const due = await this.database.query(`SELECT id${DUE_AUCTIONS}`);
    if (due.rows.length === 0) {
      return { kind: "none_due" };
    }
    return this.closeNextDue();
  }

  private async closeNextDue(): Promise<FinalizationOutcome> {
    const client = await this.database.connect();
    let transactionError: unknown;

    try {
      await client.query("BEGIN");

      // Step 1: claim. SKIP LOCKED passes over an auction another finalizer
      // (or a bid) holds, so concurrent finalizers spread over different
      // auctions instead of queueing behind one. Nothing is read from the row
      // here beyond its id: the claim may have been evaluated against a
      // snapshot older than the lock.
      const claimResult = await client.query(
        `SELECT id::text AS id${DUE_AUCTIONS} FOR UPDATE SKIP LOCKED`,
      );
      const claimed = claimResult.rows[0] as { id: string } | undefined;
      if (claimed === undefined) {
        await client.query("COMMIT");
        return { kind: "none_due" };
      }

      // Step 2: with the lock held, read the auction, its leading bid and one
      // clock instant in a fresh snapshot. A bid that committed before the
      // lock is visible here, and none can commit after it.
      const auctionResult = await client.query(
        `SELECT
           auctions.id::text AS id,
           auctions.currency_code,
           (auctions.cancelled_at IS NULL
             AND auctions.closed_at IS NULL
             AND auctions.ends_at <= observed.observed_at) AS closable,
           auctions.leading_bid_id::text AS leading_bid_id,
           leading_bid.bidder_id::text AS leading_bidder_id,
           leading_bid.amount_minor::text AS leading_amount_minor,
           to_char(
             observed.observed_at AT TIME ZONE 'UTC',
             'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
           ) AS observed_at_text
         FROM auctions
         CROSS JOIN (SELECT clock_timestamp() AS observed_at) AS observed
         LEFT JOIN bids AS leading_bid ON leading_bid.id = auctions.leading_bid_id
         WHERE auctions.id = $1`,
        [claimed.id],
      );
      const auction = auctionResult.rows[0] as ClaimedAuctionRow | undefined;
      if (auction === undefined) {
        throw new Error("Locked auction was not returned");
      }
      // Impossible while the claim holds: ends_at never changes, the clock
      // only moves forward, and only this transaction can close the row. It is
      // checked because closing twice would break the contiguous version rule.
      if (!auction.closable) {
        throw new Error(`Claimed auction ${auction.id} is no longer due`);
      }

      // The text form keeps microsecond precision; node-pg would truncate a
      // Date to milliseconds, which could put closed_at before ends_at.
      const closeResult = await client.query(
        `UPDATE auctions
         SET closed_at = $1::timestamptz,
             winning_bid_id = leading_bid_id,
             version = version + 1,
             updated_at = $1::timestamptz
         WHERE id = $2
         RETURNING version::text AS version`,
        [auction.observed_at_text, auction.id],
      );
      const closed = closeResult.rows[0] as { version: string } | undefined;
      if (closed === undefined) {
        throw new Error("Closed auction was not returned");
      }

      const event = parseAuctionClosedV1({
        eventId: randomUUID(),
        eventType: AUCTION_CLOSED_V1_EVENT_TYPE,
        occurredAt: auction.observed_at_text,
        auctionId: auction.id,
        auctionVersion: closed.version,
        currencyCode: auction.currency_code,
        winningBidId: auction.leading_bid_id,
        winnerId: auction.leading_bidder_id,
        finalAmountMinor: auction.leading_amount_minor,
      });
      await insertOutboxEvent(client, event);

      await client.query("COMMIT");
      return {
        kind: "closed",
        auctionId: auction.id,
        auctionVersion: closed.version,
        winningBidId: auction.leading_bid_id,
      };
    } catch (error) {
      transactionError = error;
      await rollbackQuietly(client);
      throw error;
    } finally {
      // A client in an unknown transaction state must not return to the pool.
      client.release(transactionError === undefined ? undefined : toError(transactionError));
    }
  }
}
