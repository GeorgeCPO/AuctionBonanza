import { randomUUID } from "node:crypto";

import { AUCTION_CLOSED_V1_EVENT_TYPE, parseAuctionClosedV1 } from "@auction-bonanza/contracts";

import { rollbackQuietly, toError, type Database, type DatabaseClient } from "../database.js";
import { insertOutboxEvent } from "../outbox/outbox.store.js";
import { backoffDelayMs } from "../shared/backoff.js";
import { describeError } from "../shared/errors.js";
import type {
  FinalizationOutcome,
  FinalizationRepository,
  FinalizationRepositoryOptions,
} from "./finalization.types.js";

interface ClaimedAuctionRow extends Record<string, unknown> {
  id: string;
  currency_code: string;
  closable: boolean;
  leading_bid_id: string | null;
  leading_bidder_id: string | null;
  leading_amount_minor: string | null;
  observed_at_text: string;
}

const MAX_ERROR_LENGTH = 1_000;

// The due-auction predicate, shared by the cheap pre-check and the locking
// claim so they cannot drift apart. An auction whose close failed is not due
// again until its scheduled retry time.
//
// The clock is read as `(SELECT clock_timestamp())` so PostgreSQL evaluates it
// once as an InitPlan parameter. A bare clock_timestamp() is volatile and
// cannot be an index condition: every poll would then walk the whole pending
// index. As written, the `ends_at` bound is an index condition on
// auctions_pending_finalization_idx and healthy auctions keep deadline order.
const DUE_AUCTIONS = `
  FROM auctions
  WHERE cancelled_at IS NULL
    AND closed_at IS NULL
    AND ends_at <= (SELECT clock_timestamp())
    AND (
      next_finalization_attempt_at IS NULL
      OR next_finalization_attempt_at <= (SELECT clock_timestamp())
    )
  ORDER BY ends_at, id
  LIMIT 1`;

// Closes auctions whose deadline has passed. Each call is one transaction for
// one auction, so a failure never holds or rolls back another auction's
// close, and row locks are held only for a few statements.
//
// The close, the version step and the AuctionClosed.v1 outbox event commit
// together or not at all. The triggers on `auctions` reject a close that
// skips any of them.
//
// A close that fails is undone with ROLLBACK TO SAVEPOINT, which keeps the
// claim's row lock, and the failure is recorded on the auction before the
// transaction commits. No other finalizer can touch the row in between, and
// the record commits although the close does not. A failure is reported as an
// outcome, not thrown: the caller moves on to the next auction.
export class PostgresFinalizationRepository implements FinalizationRepository {
  constructor(
    private readonly database: Database,
    private readonly options: FinalizationRepositoryOptions,
  ) {}

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

      // Claim. SKIP LOCKED passes over an auction another finalizer (or a bid)
      // holds, so concurrent finalizers spread over different auctions
      // instead of queueing behind one. Nothing is read from the row here
      // beyond its id: the claim may have been evaluated against a snapshot
      // older than the lock.
      const claimResult = await client.query(
        `SELECT id::text AS id${DUE_AUCTIONS} FOR UPDATE SKIP LOCKED`,
      );
      const claimed = claimResult.rows[0] as { id: string } | undefined;
      if (claimed === undefined) {
        await client.query("COMMIT");
        return { kind: "none_due" };
      }

      await client.query("SAVEPOINT close_attempt");
      let outcome: FinalizationOutcome;
      try {
        outcome = await this.close(client, claimed.id);
        await client.query("RELEASE SAVEPOINT close_attempt");
      } catch (closeError) {
        outcome = await this.recordCloseFailure(client, claimed.id, closeError);
      }

      await client.query("COMMIT");
      return outcome;
    } catch (error) {
      transactionError = error;
      await rollbackQuietly(client);
      throw error;
    } finally {
      // A client in an unknown transaction state must not return to the pool.
      client.release(transactionError === undefined ? undefined : toError(transactionError));
    }
  }

  private async close(client: DatabaseClient, auctionId: string): Promise<FinalizationOutcome> {
    // With the lock held, read the auction, its leading bid and one clock
    // instant in a fresh snapshot. A bid that committed before the lock is
    // visible here, and none can commit after it.
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
      [auctionId],
    );
    const auction = auctionResult.rows[0] as ClaimedAuctionRow | undefined;
    if (auction === undefined) {
      throw new Error("Locked auction was not returned");
    }
    // Impossible while the claim holds: ends_at never changes, the clock only
    // moves forward, and only this transaction can close the row. It is
    // checked because closing twice would break the contiguous version rule.
    if (!auction.closable) {
      throw new Error(`Claimed auction ${auction.id} is no longer due`);
    }

    // The text form keeps microsecond precision; node-pg would truncate a Date
    // to milliseconds, which could put closed_at before ends_at. A closed
    // auction is never scheduled for another attempt, but keeps its attempt
    // count and last error as history.
    const closeResult = await client.query(
      `UPDATE auctions
       SET closed_at = $1::timestamptz,
           winning_bid_id = leading_bid_id,
           version = version + 1,
           updated_at = $1::timestamptz,
           next_finalization_attempt_at = NULL
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

    // The "version has an outbox event" check is deferred to COMMIT. Running
    // it now makes a violation fail inside the savepoint, where it is
    // recorded like any other close failure, instead of failing the commit.
    await client.query("SET CONSTRAINTS ALL IMMEDIATE");

    return {
      kind: "closed",
      auctionId: auction.id,
      auctionVersion: closed.version,
      winningBidId: auction.leading_bid_id,
    };
  }

  // Undoes the failed close and schedules the next attempt. Only the retry
  // bookkeeping is written: the version, the timestamps and the outcome are
  // untouched, so no version trigger fires and no outbox event is needed.
  // Retry times use database time, like every other time-sensitive decision.
  private async recordCloseFailure(
    client: DatabaseClient,
    auctionId: string,
    closeError: unknown,
  ): Promise<FinalizationOutcome> {
    const error = describeError(closeError).slice(0, MAX_ERROR_LENGTH);
    try {
      await client.query("ROLLBACK TO SAVEPOINT close_attempt");

      const attemptsResult = await client.query(
        "SELECT finalization_attempts FROM auctions WHERE id = $1",
        [auctionId],
      );
      const attemptsRow = attemptsResult.rows[0] as { finalization_attempts: number } | undefined;
      if (attemptsRow === undefined) {
        throw new Error("Locked auction was not returned");
      }
      const attempt = attemptsRow.finalization_attempts + 1;
      const retryDelayMs = backoffDelayMs(this.options.retryBackoff, attempt, this.options.random);

      const recorded = await client.query(
        `UPDATE auctions
         SET finalization_attempts = $2,
             next_finalization_attempt_at =
               clock_timestamp() + $3::double precision * interval '1 millisecond',
             last_finalization_error = $4
         WHERE id = $1 AND closed_at IS NULL AND cancelled_at IS NULL`,
        [auctionId, attempt, retryDelayMs, error],
      );
      if (recorded.rowCount !== 1) {
        throw new Error("Failed close was not recorded");
      }
      return { kind: "close_failed", auctionId, attempt, retryDelayMs, error };
    } catch (recordError) {
      // The failure could not be recorded, usually because the connection is
      // gone. The whole cycle fails and the loop backs off.
      throw new Error(
        `Recording the failed close of auction ${auctionId} failed (close error: ${error})`,
        { cause: recordError },
      );
    }
  }
}
