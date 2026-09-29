import { randomUUID } from "node:crypto";

import { BID_ACCEPTED_V1_EVENT_TYPE, parseBidAcceptedV1 } from "@auction-bonanza/contracts";

import { derivePhase, type AuctionPhase } from "../auctions/auction.js";
import type { Database, DatabaseClient } from "../database.js";
import type { Bid } from "./bid.js";

interface BidRow extends Record<string, unknown> {
  id: string;
  auction_id: string;
  bidder_id: string;
  amount_minor: string;
  idempotency_key: string;
  created_at: Date;
}

interface AuctionLockRow extends Record<string, unknown> {
  id: string;
  owner_id: string;
  starts_at: Date;
  ends_at: Date;
  starting_price_minor: string;
  minimum_bid_increment_minor: string;
  currency_code: string;
  version: string;
  cancelled_at: Date | null;
  closed_at: Date | null;
  updated_at: Date;
  leading_bid_id: string | null;
  leading_bid_amount_minor: string | null;
  observed_at: Date;
  observed_at_text: string;
}

export interface PlaceBidInput {
  auctionId: string;
  bidderId: string;
  amountMinor: string;
  idempotencyKey: string;
}

export interface PlacedAuctionState {
  id: string;
  version: string;
  leadingBidId: string;
  updatedAt: Date;
}

export type PlaceBidPersistenceOutcome =
  | { kind: "placed"; bid: Bid; auction: PlacedAuctionState }
  | { kind: "idempotency_replay"; bid: Bid; auction: PlacedAuctionState }
  | { kind: "idempotency_key_reused"; originalAmountMinor: string }
  | { kind: "auction_not_found" }
  | { kind: "auction_not_open"; phase: Exclude<AuctionPhase, "open"> }
  | { kind: "self_bid_not_allowed" }
  | { kind: "bid_too_low"; minimumQualifyingAmountMinor: string };

export interface BidRepository {
  placeBid(input: PlaceBidInput): Promise<PlaceBidPersistenceOutcome>;
}

export class PostgresBidRepository implements BidRepository {
  constructor(private readonly database: Database) {}

  async placeBid(input: PlaceBidInput): Promise<PlaceBidPersistenceOutcome> {
    const client = await this.database.connect();
    let committed = false;

    try {
      await client.query("BEGIN");

      // Step 1: only acquire the lock. This statement may block behind another
      // bid on the same auction, so nothing time- or state-sensitive is read
      // here. In READ COMMITTED a statement's snapshot is fixed at its start,
      // so a single "lock and read" statement would evaluate clock_timestamp()
      // before the wait (admitting a bid whose deadline passed while blocked)
      // and would join `bids` against a snapshot that predates the bid the
      // other transaction just committed (reading a NULL leading amount and
      // falling back to the starting price). Locking first and reading in a
      // later statement gives both a post-lock clock and a post-lock snapshot.
      const lockResult = await client.query("SELECT id FROM auctions WHERE id = $1 FOR UPDATE", [
        input.auctionId,
      ]);
      if (lockResult.rows.length === 0) {
        await client.query("COMMIT");
        committed = true;
        return { kind: "auction_not_found" };
      }

      // Step 2: now that the lock is held, read the auction, its leading bid,
      // and clock_timestamp() in one fresh-snapshot statement. `observed` is a
      // subquery (not a repeated clock_timestamp() call) so both the Date and
      // full-precision-text projections come from the exact same instant. The
      // text form is what gets written back as created_at/updated_at: node-pg
      // truncates timestamptz to millisecond precision when parsing it into a
      // JS Date, and writing that lossy value back could violate
      // auctions_updated_at_check for a bid placed within the same
      // millisecond as its auction's created_at.
      const auctionResult = await client.query(
        `SELECT
           auctions.id::text AS id,
           auctions.owner_id::text AS owner_id,
           auctions.starts_at,
           auctions.ends_at,
           auctions.starting_price_minor::text AS starting_price_minor,
           auctions.minimum_bid_increment_minor::text AS minimum_bid_increment_minor,
           auctions.currency_code,
           auctions.version::text AS version,
           auctions.cancelled_at,
           auctions.closed_at,
           auctions.updated_at,
           auctions.leading_bid_id::text AS leading_bid_id,
           leading_bid.amount_minor::text AS leading_bid_amount_minor,
           observed.observed_at,
           to_char(
             observed.observed_at AT TIME ZONE 'UTC',
             'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
           ) AS observed_at_text
         FROM auctions
         CROSS JOIN (SELECT clock_timestamp() AS observed_at) AS observed
         LEFT JOIN bids AS leading_bid ON leading_bid.id = auctions.leading_bid_id
         WHERE auctions.id = $1`,
        [input.auctionId],
      );
      const auctionRow = auctionResult.rows[0] as AuctionLockRow | undefined;
      if (auctionRow === undefined) throw new Error("Locked auction was not returned");

      // Idempotency is checked before any other business rule: a retried
      // request must report what already happened, not be re-evaluated
      // against a since-changed price or phase.
      const existingResult = await client.query(
        `SELECT
           id::text AS id,
           auction_id::text AS auction_id,
           bidder_id::text AS bidder_id,
           amount_minor::text AS amount_minor,
           idempotency_key,
           created_at
         FROM bids
         WHERE auction_id = $1 AND bidder_id = $2 AND idempotency_key = $3`,
        [input.auctionId, input.bidderId, input.idempotencyKey],
      );
      const existingRow = existingResult.rows[0] as BidRow | undefined;
      if (existingRow !== undefined) {
        await client.query("COMMIT");
        committed = true;
        if (existingRow.amount_minor !== input.amountMinor) {
          return { kind: "idempotency_key_reused", originalAmountMinor: existingRow.amount_minor };
        }
        return {
          kind: "idempotency_replay",
          bid: mapBid(existingRow),
          auction: currentAuctionState(auctionRow),
        };
      }

      const phase = derivePhase(
        {
          cancelledAt: auctionRow.cancelled_at,
          closedAt: auctionRow.closed_at,
          startsAt: auctionRow.starts_at,
          endsAt: auctionRow.ends_at,
        },
        auctionRow.observed_at,
      );
      if (phase !== "open") {
        await client.query("COMMIT");
        committed = true;
        return { kind: "auction_not_open", phase };
      }

      if (auctionRow.owner_id === input.bidderId) {
        await client.query("COMMIT");
        committed = true;
        return { kind: "self_bid_not_allowed" };
      }

      const threshold =
        auctionRow.leading_bid_amount_minor === null
          ? BigInt(auctionRow.starting_price_minor)
          : BigInt(auctionRow.leading_bid_amount_minor) +
            BigInt(auctionRow.minimum_bid_increment_minor);
      // Bids must be strictly positive even when starting_price_minor is 0.
      const minimumQualifying = threshold < 1n ? 1n : threshold;
      if (BigInt(input.amountMinor) < minimumQualifying) {
        await client.query("COMMIT");
        committed = true;
        return {
          kind: "bid_too_low",
          minimumQualifyingAmountMinor: minimumQualifying.toString(),
        };
      }

      const insertedResult = await client.query(
        `INSERT INTO bids (auction_id, bidder_id, amount_minor, idempotency_key, created_at)
         VALUES ($1, $2, $3, $4, $5::timestamptz)
         RETURNING
           id::text AS id,
           auction_id::text AS auction_id,
           bidder_id::text AS bidder_id,
           amount_minor::text AS amount_minor,
           idempotency_key,
           created_at`,
        [
          input.auctionId,
          input.bidderId,
          input.amountMinor,
          input.idempotencyKey,
          auctionRow.observed_at_text,
        ],
      );
      const bidRow = insertedResult.rows[0] as BidRow | undefined;
      if (bidRow === undefined) throw new Error("Inserted bid was not returned");

      const updatedResult = await client.query(
        `UPDATE auctions
         SET leading_bid_id = $1::bigint,
             version = version + 1,
             updated_at = $2::timestamptz
         WHERE id = $3
         RETURNING version::text AS version, updated_at`,
        [bidRow.id, auctionRow.observed_at_text, input.auctionId],
      );
      const updatedAuctionRow = updatedResult.rows[0] as
        | { version: string; updated_at: Date }
        | undefined;
      if (updatedAuctionRow === undefined) throw new Error("Updated auction was not returned");

      const event = parseBidAcceptedV1({
        eventId: randomUUID(),
        eventType: BID_ACCEPTED_V1_EVENT_TYPE,
        occurredAt: auctionRow.observed_at_text,
        auctionId: input.auctionId,
        auctionVersion: updatedAuctionRow.version,
        bidId: bidRow.id,
        amountMinor: bidRow.amount_minor,
        currencyCode: auctionRow.currency_code,
      });
      // The event must carry the auction version this transaction just
      // wrote: the outbox_events_current_auction_version trigger rejects any
      // other version, which is what keeps per-auction publication ordered.
      await client.query(
        `INSERT INTO outbox_events (
           event_id, event_type, occurred_at, auction_id, auction_version, payload
         )
         VALUES ($1::uuid, $2, $3::timestamptz, $4, $5, $6::jsonb)`,
        [
          event.eventId,
          event.eventType,
          event.occurredAt,
          event.auctionId,
          event.auctionVersion,
          JSON.stringify(event),
        ],
      );

      await client.query("COMMIT");
      committed = true;
      return {
        kind: "placed",
        bid: mapBid(bidRow),
        auction: {
          id: input.auctionId,
          version: updatedAuctionRow.version,
          leadingBidId: bidRow.id,
          updatedAt: updatedAuctionRow.updated_at,
        },
      };
    } catch (error) {
      if (!committed) {
        await rollback(client);
      }
      throw error;
    } finally {
      client.release();
    }
  }
}

function mapBid(row: BidRow): Bid {
  return {
    id: row.id,
    auctionId: row.auction_id,
    bidderId: row.bidder_id,
    amountMinor: row.amount_minor,
    idempotencyKey: row.idempotency_key,
    createdAt: row.created_at,
  };
}

// The leading bid may since have moved to someone else's higher bid — this
// reports the auction's current state, not the state at the time the
// replayed bid was originally accepted. `leading_bid_id` cannot be null here:
// an existing bid row for this auction was already found by the caller, so
// at least one bid (this one, or a later one that overtook it) must have
// been accepted and recorded as leading.
function currentAuctionState(row: AuctionLockRow): PlacedAuctionState {
  if (row.leading_bid_id === null) {
    throw new Error("Auction has an existing bid but no leading_bid_id");
  }
  return {
    id: row.id,
    version: row.version,
    leadingBidId: row.leading_bid_id,
    updatedAt: row.updated_at,
  };
}

async function rollback(client: DatabaseClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch {
    // Preserve the original transaction error; the checked-out client is
    // released immediately and pg will discard it if the connection failed.
  }
}
