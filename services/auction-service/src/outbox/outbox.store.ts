import type { DatabaseClient } from "../database.js";

// Outbox publication state. Every function runs on a client the caller has
// already placed inside a transaction; the row locks taken by
// claimPublishableEvents are held until that transaction ends.

const MAX_PUBLISH_ERROR_LENGTH = 1_000;

export interface ClaimedOutboxEvent {
  eventId: string;
  eventType: string;
  auctionId: string;
  auctionVersion: string;
  // PostgreSQL's canonical text rendering of the stored jsonb payload. The
  // same stored value always renders to the same text, so every publication
  // attempt of an event carries byte-identical content.
  payloadJson: string;
  publishAttempts: number;
}

export interface PublicationFailure {
  eventId: string;
  retryDelayMs: number;
  error: string;
}

interface ClaimedRow extends Record<string, unknown> {
  event_id: string;
  event_type: string;
  auction_id: string;
  auction_version: string;
  payload_json: string;
  publish_attempts: number;
}

// Claims up to `limit` publishable events: unpublished, due, and the earliest
// unpublished event of their auction, so at most one per auction. Two details
// are deliberate. The NOT EXISTS predecessor check ignores retry scheduling
// and row locks, so a predecessor that is waiting to retry, or is being sent
// by another publisher, still blocks its successors. And rows skipped by
// SKIP LOCKED do not count toward LIMIT, so one busy auction cannot starve the
// batch. Why this cannot reorder an auction's events is explained under
// "Claiming" in docs/OUTBOX_PUBLISHING.md.
export async function claimPublishableEvents(
  client: DatabaseClient,
  limit: number,
): Promise<ClaimedOutboxEvent[]> {
  const result = await client.query(
    `SELECT
       candidate.event_id::text AS event_id,
       candidate.event_type,
       candidate.auction_id::text AS auction_id,
       candidate.auction_version::text AS auction_version,
       candidate.payload::text AS payload_json,
       candidate.publish_attempts
     FROM outbox_events AS candidate
     WHERE candidate.published_at IS NULL
       AND (
         candidate.next_publish_attempt_at IS NULL
         OR candidate.next_publish_attempt_at <= clock_timestamp()
       )
       AND NOT EXISTS (
         SELECT 1
         FROM outbox_events AS predecessor
         WHERE predecessor.auction_id = candidate.auction_id
           AND predecessor.published_at IS NULL
           AND predecessor.auction_version < candidate.auction_version
       )
     ORDER BY candidate.occurred_at, candidate.event_id
     LIMIT $1
     FOR UPDATE OF candidate SKIP LOCKED`,
    [limit],
  );

  return (result.rows as ClaimedRow[]).map((row) => ({
    eventId: row.event_id,
    eventType: row.event_type,
    auctionId: row.auction_id,
    auctionVersion: row.auction_version,
    payloadJson: row.payload_json,
    publishAttempts: row.publish_attempts,
  }));
}

export async function markEventsPublished(
  client: DatabaseClient,
  eventIds: readonly string[],
): Promise<void> {
  if (eventIds.length === 0) return;

  const result = await client.query(
    `UPDATE outbox_events
     SET published_at = clock_timestamp(),
         next_publish_attempt_at = NULL
     WHERE event_id = ANY($1::uuid[]) AND published_at IS NULL`,
    [eventIds],
  );
  // The rows are locked by this transaction, so nobody else can have
  // published them. Anything else means the caller lost its claim.
  if (result.rowCount !== eventIds.length) {
    throw new Error(
      `Expected to mark ${eventIds.length} claimed outbox events published, marked ${result.rowCount ?? 0}`,
    );
  }
}

export async function recordPublicationFailures(
  client: DatabaseClient,
  failures: readonly PublicationFailure[],
): Promise<void> {
  if (failures.length === 0) return;

  const result = await client.query(
    `UPDATE outbox_events AS event
     SET publish_attempts = event.publish_attempts + 1,
         next_publish_attempt_at =
           clock_timestamp() + failure.retry_delay_ms * interval '1 millisecond',
         last_publish_error = failure.error
     FROM unnest($1::uuid[], $2::bigint[], $3::text[])
       AS failure(event_id, retry_delay_ms, error)
     WHERE event.event_id = failure.event_id AND event.published_at IS NULL`,
    [
      failures.map((failure) => failure.eventId),
      failures.map((failure) => String(failure.retryDelayMs)),
      failures.map((failure) => failure.error.slice(0, MAX_PUBLISH_ERROR_LENGTH)),
    ],
  );
  if (result.rowCount !== failures.length) {
    throw new Error(
      `Expected to record ${failures.length} outbox publication failures, recorded ${result.rowCount ?? 0}`,
    );
  }
}
