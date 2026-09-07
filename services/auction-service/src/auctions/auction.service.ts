import type { Auction } from "./auction.js";
import type { AuctionRepository } from "./auction.repository.js";

const POSTGRES_BIGINT_MAX = 9_223_372_036_854_775_807n;

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

export interface AuctionServicePort {
  create(ownerId: string, input: CreateAuctionInput): Promise<CreateAuctionOutcome>;
  findById(id: string): Promise<Auction | undefined>;
}

function characterLength(value: string): number {
  return Array.from(value).length;
}

function parseMinorUnits(value: string, allowZero: boolean): string | undefined {
  if (!/^(0|[1-9][0-9]*)$/.test(value)) return undefined;
  const amount = BigInt(value);
  if (amount > POSTGRES_BIGINT_MAX || (!allowZero && amount === 0n)) return undefined;
  return amount.toString();
}

function parseTimestamp(value: string): Date | undefined {
  const timestamp = new Date(value);
  return Number.isNaN(timestamp.getTime()) ? undefined : timestamp;
}

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
}

export function isPostgresBigintId(value: string): boolean {
  if (!/^[1-9][0-9]*$/.test(value)) return false;
  return BigInt(value) <= POSTGRES_BIGINT_MAX;
}
