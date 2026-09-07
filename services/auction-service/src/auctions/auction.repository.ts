import type { Database } from "../database.js";
import type { Auction } from "./auction.js";

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

export type AuctionCreationPersistenceOutcome =
  | { kind: "created"; auction: Auction }
  | { kind: "invalid_schedule" };

export interface AuctionRepository {
  create(request: PersistAuctionCreation): Promise<AuctionCreationPersistenceOutcome>;
  findById(id: string): Promise<Auction | undefined>;
}

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
         RETURNING
           id::text AS id,
           owner_id::text AS owner_id,
           title, description, currency_code, starts_at, ends_at,
           starting_price_minor::text AS starting_price_minor,
           minimum_bid_increment_minor::text AS minimum_bid_increment_minor,
           version::text AS version,
           cancelled_at, closed_at, created_at, updated_at,
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
      if (row === undefined) throw new Error("Inserted auction was not returned");
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
      `SELECT
       a.id::text AS id,
       a.owner_id::text AS owner_id,
       a.title,
       a.description,
       a.currency_code,
       a.starts_at,
       a.ends_at,
       a.starting_price_minor::text AS starting_price_minor,
       a.minimum_bid_increment_minor::text AS minimum_bid_increment_minor,
       a.version::text AS version,
       a.cancelled_at,
       a.closed_at,
       a.created_at,
       a.updated_at,
       clock_timestamp() AS observed_at
     FROM auctions a
     WHERE a.id = $1`,
      [id],
    );
    const row = result.rows[0] as AuctionRow | undefined;
    return row === undefined ? undefined : this.mapAuction(row);
  }

  private mapAuction(row: AuctionRow): Auction {
    let phase: Auction["phase"] = "awaiting_finalization";
    if (row.cancelled_at !== null) {
      phase = "cancelled";
    } else if (row.closed_at !== null) {
      phase = "closed";
    } else if (row.observed_at < row.starts_at) {
      phase = "scheduled";
    } else if (row.observed_at < row.ends_at) {
      phase = "open";
    }

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
}
