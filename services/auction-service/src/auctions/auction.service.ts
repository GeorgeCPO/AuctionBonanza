import { isPostgresBigintId, parseMinorUnits } from "../shared/money.js";
import type { Auction, AuctionLiveState } from "./auction.js";
import type { AuctionListCursor, AuctionRepository } from "./auction.repository.js";

const DEFAULT_AUCTION_LIST_LIMIT = 20;
const MAX_AUCTION_LIST_LIMIT = 100;
// Bounds one bulk live-state read, which realtime edges issue when they
// resync many auctions at once.
export const MAX_LIVE_STATE_IDS = 100;
const CURSOR_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/;

// Shared with the controller's querystring schema so a cursor too long to be
// valid is rejected at the HTTP boundary instead of reaching this parser.
export const MAX_AUCTION_LIST_CURSOR_LENGTH = 256;

export interface CreateAuctionInput {
  title: string;
  description: string;
  currencyCode: string;
  startsAt?: string;
  endsAt: string;
  startingPriceMinor: string;
  minimumBidIncrementMinor: string;
}

export type CreateAuctionOutcome =
  | { kind: "created"; auction: Auction }
  | { kind: "invalid"; message: string };

export interface ListAuctionsInput {
  search?: string;
  limit?: string;
  cursor?: string | undefined;
}

export interface AuctionPage {
  items: Auction[];
  nextCursor: string | null;
}

export type ListAuctionsOutcome =
  | { kind: "found"; page: AuctionPage }
  | { kind: "invalid"; message: string };

export interface AuctionLiveStates {
  items: AuctionLiveState[];
  missingIds: string[];
  observedAt: Date;
}

export type FindLiveStatesOutcome =
  | { kind: "found"; liveStates: AuctionLiveStates }
  | { kind: "invalid"; message: string };

export interface AuctionServicePort {
  create(ownerId: string, input: CreateAuctionInput): Promise<CreateAuctionOutcome>;
  findById(id: string): Promise<Auction | undefined>;
  list(input: ListAuctionsInput): Promise<ListAuctionsOutcome>;
  findLiveStates(ids: string): Promise<FindLiveStatesOutcome>;
}

function characterLength(value: string): number {
  return Array.from(value).length;
}

function parseTimestamp(value: string): Date | undefined {
  const timestamp = new Date(value);
  return Number.isNaN(timestamp.getTime()) ? undefined : timestamp;
}

type CursorParseResult =
  | { kind: "absent" }
  | { kind: "invalid" }
  | { kind: "present"; cursor: AuctionListCursor };

export class AuctionService implements AuctionServicePort {
  readonly #supportedCurrencyCodes: ReadonlySet<string>;

  constructor(
    private readonly repository: AuctionRepository,
    supportedCurrencyCodes: readonly string[],
  ) {
    this.#supportedCurrencyCodes = new Set(supportedCurrencyCodes);
  }

  async create(ownerId: string, input: CreateAuctionInput): Promise<CreateAuctionOutcome> {
    const title = input.title.trim();
    if (characterLength(title) < 1 || characterLength(title) > 160) {
      return { kind: "invalid", message: "title must contain between 1 and 160 characters" };
    }

    const description = input.description.trim();
    if (characterLength(description) < 1 || characterLength(description) > 10_000) {
      return {
        kind: "invalid",
        message: "description must contain between 1 and 10000 characters",
      };
    }

    if (!this.#supportedCurrencyCodes.has(input.currencyCode)) {
      return { kind: "invalid", message: "currencyCode is not supported" };
    }

    const startingPriceMinor = parseMinorUnits(input.startingPriceMinor, true);
    if (startingPriceMinor === undefined) {
      return { kind: "invalid", message: "startingPriceMinor is outside the supported range" };
    }
    const minimumBidIncrementMinor = parseMinorUnits(input.minimumBidIncrementMinor, false);
    if (minimumBidIncrementMinor === undefined) {
      return {
        kind: "invalid",
        message: "minimumBidIncrementMinor must be positive and within the supported range",
      };
    }

    const startsAt = input.startsAt === undefined ? null : parseTimestamp(input.startsAt);
    const endsAt = parseTimestamp(input.endsAt);
    if (startsAt === undefined || endsAt === undefined) {
      return { kind: "invalid", message: "startsAt and endsAt must be valid timestamps" };
    }

    const outcome = await this.repository.create({
      ownerId,
      title,
      description,
      currencyCode: input.currencyCode,
      startsAt,
      endsAt,
      startingPriceMinor,
      minimumBidIncrementMinor,
    });

    if (outcome.kind === "invalid_schedule") {
      return {
        kind: "invalid",
        message: "The auction duration must be between 1 minute and 30 days",
      };
    }
    return outcome;
  }

  findById(id: string): Promise<Auction | undefined> {
    return this.repository.findById(id);
  }

  async list(input: ListAuctionsInput): Promise<ListAuctionsOutcome> {
    const limit = this.parseLimit(input.limit);
    if (limit === undefined) {
      return { kind: "invalid", message: "limit must be an integer between 1 and 100" };
    }

    const parsedCursor = this.parseCursor(input.cursor);
    if (parsedCursor.kind === "invalid") {
      return { kind: "invalid", message: "cursor is invalid" };
    }

    const search = input.search?.trim();
    const entries = await this.repository.list({
      search: search === "" ? undefined : search,
      limit: limit + 1,
      cursor: parsedCursor.kind === "present" ? parsedCursor.cursor : undefined,
    });
    const hasNextPage = entries.length > limit;
    const pageEntries = hasNextPage ? entries.slice(0, limit) : entries;
    const lastEntry = pageEntries.at(-1);

    return {
      kind: "found",
      page: {
        items: pageEntries.map((entry) => entry.auction),
        nextCursor:
          hasNextPage && lastEntry !== undefined ? this.encodeCursor(lastEntry.cursor) : null,
      },
    };
  }

  async findLiveStates(ids: string): Promise<FindLiveStatesOutcome> {
    const parsedIds = this.parseLiveStateIds(ids);
    if (parsedIds === undefined) {
      return {
        kind: "invalid",
        message: `ids must be 1 to ${MAX_LIVE_STATE_IDS} comma-separated auction IDs`,
      };
    }

    const snapshot = await this.repository.findLiveStates(parsedIds);
    const statesById = new Map(snapshot.states.map((state) => [state.auctionId, state]));
    const items: AuctionLiveState[] = [];
    const missingIds: string[] = [];
    for (const id of parsedIds) {
      const state = statesById.get(id);
      if (state === undefined) missingIds.push(id);
      else items.push(state);
    }
    return { kind: "found", liveStates: { items, missingIds, observedAt: snapshot.observedAt } };
  }

  // Returns the distinct IDs in request order. A repeated ID is accepted once,
  // since asking twice for the same state is harmless.
  private parseLiveStateIds(value: string): string[] | undefined {
    const ids = [...new Set(value.split(","))];
    if (ids.length > MAX_LIVE_STATE_IDS) return undefined;
    if (!ids.every((id) => isPostgresBigintId(id))) return undefined;
    return ids;
  }

  private parseLimit(value: string | undefined): number | undefined {
    if (value === undefined) return DEFAULT_AUCTION_LIST_LIMIT;
    if (!/^[1-9][0-9]*$/.test(value)) return undefined;

    const limit = Number(value);
    if (!Number.isSafeInteger(limit) || limit > MAX_AUCTION_LIST_LIMIT) return undefined;
    return limit;
  }

  private parseCursor(value: string | undefined): CursorParseResult {
    if (value === undefined) return { kind: "absent" };
    if (value.length > MAX_AUCTION_LIST_CURSOR_LENGTH || !/^[A-Za-z0-9_-]+$/.test(value)) {
      return { kind: "invalid" };
    }

    try {
      const decoded = Buffer.from(value, "base64url").toString("utf8");
      if (Buffer.from(decoded, "utf8").toString("base64url") !== value) return { kind: "invalid" };

      const candidate: unknown = JSON.parse(decoded);
      if (typeof candidate !== "object" || candidate === null) return { kind: "invalid" };
      if (!("createdAt" in candidate) || !("id" in candidate)) return { kind: "invalid" };
      if (Object.keys(candidate).length !== 2) return { kind: "invalid" };

      const { createdAt, id } = candidate;
      if (typeof createdAt !== "string" || !CURSOR_TIMESTAMP_PATTERN.test(createdAt)) {
        return { kind: "invalid" };
      }
      const timestamp = new Date(createdAt);
      if (Number.isNaN(timestamp.getTime())) return { kind: "invalid" };
      // Date is used only to validate the calendar, never to rebuild the cursor.
      if (timestamp.toISOString() !== `${createdAt.slice(0, 23)}Z`) return { kind: "invalid" };
      if (createdAt.startsWith("0000-")) return { kind: "invalid" };
      if (typeof id !== "string" || !isPostgresBigintId(id)) return { kind: "invalid" };

      return { kind: "present", cursor: { createdAt, id } };
    } catch {
      return { kind: "invalid" };
    }
  }

  private encodeCursor(cursor: AuctionListCursor): string {
    return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
  }
}
