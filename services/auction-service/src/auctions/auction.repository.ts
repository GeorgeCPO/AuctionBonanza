import type { Database } from "../database.js";
import { derivePhase, type Auction, type AuctionLiveState } from "./auction.js";

interface AuctionRow extends Record<string, unknown> {
  id: string;
  owner_id: string;
  title: string;
  description: string;
  currency_code: string;
  starts_at: Date;
  ends_at: Date;
  starting_price_minor: string;
  minimum_bid_increment_minor: string;
  version: string;
  cancelled_at: Date | null;
  closed_at: Date | null;
  created_at: Date;
  updated_at: Date;
  observed_at: Date;
}

export interface PersistAuctionCreation {
  ownerId: string;
  title: string;
  description: string;
  currencyCode: string;
  startsAt: Date | null;
  endsAt: Date;
  startingPriceMinor: string;
  minimumBidIncrementMinor: string;
}

interface AuctionListRow extends AuctionRow {
  cursor_created_at: string;
}

export interface AuctionListCursor {
  createdAt: string;
  id: string;
}

export interface ListAuctionsQuery {
  search?: string | undefined;
  limit: number;
  cursor?: AuctionListCursor | undefined;
}

export interface AuctionListEntry {
  auction: Auction;
  cursor: AuctionListCursor;
}

interface AuctionLiveStateRow extends Record<string, unknown> {
  auction_id: string | null;
  auction_version: string | null;
  currency_code: string | null;
  starts_at: Date | null;
  ends_at: Date | null;
  cancelled_at: Date | null;
  closed_at: Date | null;
  leading_bid_id: string | null;
  leading_bid_amount_minor: string | null;
  observed_at: Date;
}

export interface AuctionLiveStatesSnapshot {
  states: AuctionLiveState[];
  observedAt: Date;
}

export type AuctionCreationPersistenceOutcome =
  | { kind: "created"; auction: Auction }
  | { kind: "invalid_schedule" };

export interface AuctionRepository {
  create(request: PersistAuctionCreation): Promise<AuctionCreationPersistenceOutcome>;
  findById(id: string): Promise<Auction | undefined>;
  list(query: ListAuctionsQuery): Promise<AuctionListEntry[]>;
  findLiveStates(ids: readonly string[]): Promise<AuctionLiveStatesSnapshot>;
}

// Shared by create/findById/list, which all read the same auction columns
// (each pairs it with its own observed_at derivation and row source).
const AUCTION_COLUMNS = `
  id::text AS id,
  owner_id::text AS owner_id,
  title,
  description,
  currency_code,
  starts_at,
  ends_at,
  starting_price_minor::text AS starting_price_minor,
  minimum_bid_increment_minor::text AS minimum_bid_increment_minor,
  version::text AS version,
  cancelled_at,
  closed_at,
  created_at,
  updated_at`;

export class PostgresAuctionRepository implements AuctionRepository {
  constructor(private readonly database: Database) {}

  async create(request: PersistAuctionCreation): Promise<AuctionCreationPersistenceOutcome> {
    try {
      const inserted = await this.database.query(
        `INSERT INTO auctions (
           owner_id,
           title,
           description,
           currency_code,
           starts_at,
           ends_at,
           starting_price_minor,
           minimum_bid_increment_minor
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           COALESCE($5::timestamptz, clock_timestamp()),
           $6::timestamptz,
           $7,
           $8
         )
         RETURNING${AUCTION_COLUMNS},
           clock_timestamp() AS observed_at`,
        [
          request.ownerId,
          request.title,
          request.description,
          request.currencyCode,
          request.startsAt,
          request.endsAt,
          request.startingPriceMinor,
          request.minimumBidIncrementMinor,
        ],
      );
      const row = inserted.rows[0] as AuctionRow | undefined;
      if (row === undefined) {
        throw new Error("Inserted auction was not returned");
      }
      return { kind: "created", auction: this.mapAuction(row) };
    } catch (error) {
      if (
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        error.code === "23514" &&
        "constraint" in error &&
        (error.constraint === "auctions_schedule_check" ||
          error.constraint === "auctions_duration_check")
      ) {
        return { kind: "invalid_schedule" };
      }
      throw error;
    }
  }

  async findById(id: string): Promise<Auction | undefined> {
    const result = await this.database.query(
      `SELECT${AUCTION_COLUMNS},
       clock_timestamp() AS observed_at
     FROM auctions
     WHERE id = $1`,
      [id],
    );
    const row = result.rows[0] as AuctionRow | undefined;
    return row === undefined ? undefined : this.mapAuction(row);
  }

  async list(query: ListAuctionsQuery): Promise<AuctionListEntry[]> {
    const result = await this.database.query(
      `SELECT${AUCTION_COLUMNS},
       -- Wrapped in an uncorrelated subquery so PostgreSQL evaluates this
       -- volatile call once per query (an InitPlan) instead of once per row,
       -- keeping every row on the page derived from the same observed instant.
       (SELECT clock_timestamp()) AS observed_at,
       to_char(
         created_at AT TIME ZONE 'UTC',
         'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
       ) AS cursor_created_at
     FROM auctions
     WHERE ($1::text IS NULL OR strpos(lower(title), lower($1)) > 0)
       AND (
         $2::timestamptz IS NULL
         OR (created_at, id) < ($2::timestamptz, $3::bigint)
       )
     ORDER BY created_at DESC, id DESC
     LIMIT $4`,
      [
        query.search ?? null,
        query.cursor?.createdAt ?? null,
        query.cursor?.id ?? null,
        query.limit,
      ],
    );

    return result.rows.map((row) => {
      const auctionRow = row as AuctionListRow;
      return {
        auction: this.mapAuction(auctionRow),
        cursor: { createdAt: auctionRow.cursor_created_at, id: auctionRow.id },
      };
    });
  }

  async findLiveStates(ids: readonly string[]): Promise<AuctionLiveStatesSnapshot> {
    // One statement, so every row comes from one snapshot. Each auction's
    // version and leading_bid_id are read from the same row version, and the
    // referenced bid is immutable, so a state can never pair one version with
    // the leading bid of another. The observed instant is the outer row so
    // that it is returned even when no requested auction exists.
    const result = await this.database.query(
      `SELECT auctions.id::text AS auction_id,
              auctions.version::text AS auction_version,
              auctions.currency_code,
              auctions.starts_at,
              auctions.ends_at,
              auctions.cancelled_at,
              auctions.closed_at,
              bids.id::text AS leading_bid_id,
              bids.amount_minor::text AS leading_bid_amount_minor,
              observed.at AS observed_at
       FROM (SELECT clock_timestamp() AS at) AS observed
       LEFT JOIN (
         auctions
         LEFT JOIN bids ON bids.id = auctions.leading_bid_id
       ) ON auctions.id = ANY($1::bigint[])`,
      [ids],
    );
    const rows = result.rows as AuctionLiveStateRow[];
    const observedAt = rows[0]?.observed_at;
    if (observedAt === undefined) {
      throw new Error("Live-state query returned no rows");
    }

    const states: AuctionLiveState[] = [];
    for (const row of rows) {
      if (row.auction_id === null) {
        continue;
      }
      states.push(this.mapLiveState(row, observedAt));
    }
    return { states, observedAt };
  }

  private mapAuction(row: AuctionRow): Auction {
    const phase = derivePhase(
      {
        cancelledAt: row.cancelled_at,
        closedAt: row.closed_at,
        startsAt: row.starts_at,
        endsAt: row.ends_at,
      },
      row.observed_at,
    );

    return {
      id: row.id,
      ownerId: row.owner_id,
      title: row.title,
      description: row.description,
      currencyCode: row.currency_code,
      startsAt: row.starts_at,
      endsAt: row.ends_at,
      startingPriceMinor: row.starting_price_minor,
      minimumBidIncrementMinor: row.minimum_bid_increment_minor,
      version: row.version,
      cancelledAt: row.cancelled_at,
      closedAt: row.closed_at,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      phase,
    };
  }

  private mapLiveState(row: AuctionLiveStateRow, observedAt: Date): AuctionLiveState {
    const { auction_id, auction_version, currency_code, starts_at, ends_at } = row;
    if (
      auction_id === null ||
      auction_version === null ||
      currency_code === null ||
      starts_at === null ||
      ends_at === null
    ) {
      throw new Error("Live-state row is missing auction columns");
    }

    const phase = derivePhase(
      {
        cancelledAt: row.cancelled_at,
        closedAt: row.closed_at,
        startsAt: starts_at,
        endsAt: ends_at,
      },
      observedAt,
    );
    const leadingBid =
      row.leading_bid_id === null || row.leading_bid_amount_minor === null
        ? null
        : { bidId: row.leading_bid_id, amountMinor: row.leading_bid_amount_minor };

    return {
      auctionId: auction_id,
      auctionVersion: auction_version,
      currencyCode: currency_code,
      startsAt: starts_at,
      endsAt: ends_at,
      cancelledAt: row.cancelled_at,
      closedAt: row.closed_at,
      phase,
      leadingBid,
    };
  }
}
