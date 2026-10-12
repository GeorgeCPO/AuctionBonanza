import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import type { Pool } from "pg";
import { pino, type Logger } from "pino";

import { PostgresBidRepository } from "../../src/bids/bid.repository.js";
import { wrapPool } from "../../src/database.js";
import type {
  DeliveryReceipt,
  MessageTransport,
  OutboundMessage,
} from "../../src/outbox/outbox.transport.js";

export interface OutboxRow {
  eventId: string;
  eventType: string;
  auctionId: string;
  auctionVersion: string;
  payloadJson: string;
  publishedAt: Date | null;
  publishAttempts: number;
  nextPublishAttemptAt: Date | null;
  lastPublishError: string | null;
}

// Produces outbox events through the real bid-acceptance transaction, so the
// publisher is exercised against exactly what the first increment writes.
export class AuctionFixtures {
  private readonly bids: PostgresBidRepository;
  private nextAmountByAuction = new Map<string, bigint>();

  constructor(private readonly pool: Pool) {
    this.bids = new PostgresBidRepository(wrapPool(pool));
  }

  async user(): Promise<string> {
    const result = await this.pool.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'Outbox Tester', '$argon2id$test')
       RETURNING id::text`,
      [`outbox-publisher-${randomUUID()}@example.com`],
    );
    return result.rows[0]!.id;
  }

  async openAuction(ownerId: string): Promise<string> {
    const result = await this.pool.query<{ id: string }>(
      `INSERT INTO auctions (
         owner_id, title, description, currency_code, starts_at, ends_at,
         starting_price_minor, minimum_bid_increment_minor
       )
       VALUES ($1, 'Outbox lot', 'An outbox publisher fixture.', 'EUR',
               now() - interval '1 minute', now() + interval '1 hour', 1000, 100)
       RETURNING id::text`,
      [ownerId],
    );
    const auctionId = result.rows[0]!.id;
    this.nextAmountByAuction.set(auctionId, 1_000n);
    return auctionId;
  }

  // Moves an auction's schedule into the past, as if its deadline had
  // arrived. Bids are placed while it is open, through the real acceptance
  // transaction, and the auction is expired afterwards.
  async expire(auctionId: string): Promise<void> {
    await this.pool.query(
      `UPDATE auctions
       SET starts_at = clock_timestamp() - interval '2 hours',
           ends_at = clock_timestamp() - interval '1 second'
       WHERE id = $1`,
      [auctionId],
    );
  }

  // Accepts the next qualifying bid and returns the resulting outbox event ID.
  async acceptBid(auctionId: string, bidderId: string): Promise<string> {
    const amount = this.nextAmountByAuction.get(auctionId);
    if (amount === undefined) {
      throw new Error(`Unknown fixture auction ${auctionId}`);
    }
    const outcome = await this.bids.placeBid({
      auctionId,
      bidderId,
      amountMinor: amount.toString(),
      idempotencyKey: randomUUID(),
    });
    if (outcome.kind !== "placed") {
      throw new Error(`Fixture bid was not placed: ${outcome.kind}`);
    }
    this.nextAmountByAuction.set(auctionId, amount + 100n);

    const result = await this.pool.query<{ event_id: string }>(
      `SELECT event_id::text FROM outbox_events WHERE auction_id = $1 AND auction_version = $2`,
      [auctionId, outcome.auction.version],
    );
    return result.rows[0]!.event_id;
  }

  // Appends an event with a well-formed envelope but no publication
  // contract, at the auction's current version. Returns its event ID.
  async insertUnpublishableEvent(auctionId: string, eventType = "Unknown.v1"): Promise<string> {
    const eventId = randomUUID();
    await this.pool.query(
      `INSERT INTO outbox_events (
         event_id, event_type, occurred_at, auction_id, auction_version, payload
       )
       SELECT $1::uuid, $3::text, observed.at, auctions.id, auctions.version,
              jsonb_build_object(
                'eventId', $1::text,
                'eventType', $3::text,
                'occurredAt',
                  to_char(observed.at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                'auctionId', auctions.id::text,
                'auctionVersion', auctions.version::text
              )
       FROM auctions
       CROSS JOIN (SELECT clock_timestamp() AS at) AS observed
       WHERE auctions.id = $2`,
      [eventId, auctionId, eventType],
    );
    return eventId;
  }

  async outboxRow(eventId: string): Promise<OutboxRow> {
    const result = await this.pool.query<Record<string, unknown>>(
      `SELECT event_id::text, event_type, auction_id::text, auction_version::text,
              payload::text AS payload_json, published_at, publish_attempts,
              next_publish_attempt_at, last_publish_error
       FROM outbox_events WHERE event_id = $1`,
      [eventId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`No outbox event ${eventId}`);
    }
    return {
      eventId: row.event_id as string,
      eventType: row.event_type as string,
      auctionId: row.auction_id as string,
      auctionVersion: row.auction_version as string,
      payloadJson: row.payload_json as string,
      publishedAt: row.published_at as Date | null,
      publishAttempts: row.publish_attempts as number,
      nextPublishAttemptAt: row.next_publish_attempt_at as Date | null,
      lastPublishError: row.last_publish_error as string | null,
    };
  }

  // Simulates the passage of time for events waiting for a retry.
  async makeRetriesDue(): Promise<void> {
    await this.pool.query(
      `UPDATE outbox_events
       SET next_publish_attempt_at = clock_timestamp() - interval '1 millisecond'
       WHERE published_at IS NULL AND next_publish_attempt_at IS NOT NULL`,
    );
  }
}

// Wraps the real Kafka transport with deterministic hooks around each send.
export class ScriptedTransport implements MessageTransport {
  readonly attempted: OutboundMessage[] = [];
  beforeSend: ((message: OutboundMessage) => Promise<void> | void) | undefined;
  afterAcknowledgement:
    | ((message: OutboundMessage, receipt: DeliveryReceipt) => Promise<void> | void)
    | undefined;

  constructor(private readonly inner: MessageTransport) {}

  async send(message: OutboundMessage): Promise<DeliveryReceipt> {
    this.attempted.push(message);
    await this.beforeSend?.(message);
    const receipt = await this.inner.send(message);
    await this.afterAcknowledgement?.(message, receipt);
    return receipt;
  }
}

// Holds callers until opened, and reports how many are waiting.
export class Gate {
  private waiting = 0;
  private opened = false;
  private release: () => void = () => {};
  private readonly openPromise = new Promise<void>((resolve) => (this.release = resolve));
  private arrivalListeners: (() => void)[] = [];

  async pass(): Promise<void> {
    if (this.opened) {
      return;
    }
    this.waiting += 1;
    const listeners = this.arrivalListeners;
    this.arrivalListeners = [];
    for (const listener of listeners) {
      listener();
    }
    await this.openPromise;
  }

  async arrivals(count: number): Promise<void> {
    while (this.waiting < count) {
      await new Promise<void>((resolve) => this.arrivalListeners.push(resolve));
    }
  }

  open(): void {
    this.opened = true;
    this.release();
  }
}

export interface CapturedLogger {
  logger: Logger;
  entries: Record<string, unknown>[];
}

export function captureLogger(): CapturedLogger {
  const entries: Record<string, unknown>[] = [];
  const logger = pino(
    { level: "debug" },
    { write: (line: string) => void entries.push(JSON.parse(line) as Record<string, unknown>) },
  );
  return { logger, entries };
}

export async function waitFor(condition: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() > deadline) {
      throw new Error(`Condition not met within ${timeoutMs}ms`);
    }
    await delay(50);
  }
}

export function byText(left: string, right: string): number {
  return left.localeCompare(right);
}
