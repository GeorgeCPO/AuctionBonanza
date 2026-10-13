import {
  AUCTION_CLOSED_V1_EVENT_TYPE,
  BID_ACCEPTED_V1_EVENT_TYPE,
  EVENT_ID_HEADER,
  EVENT_TYPE_HEADER,
  parseAuctionClosedV1,
  parseBidAcceptedV1,
} from "@auction-bonanza/contracts";
import type { Logger } from "pino";

import { rollbackQuietly, toError, type Database } from "../database.js";
import { backoffDelayMs, type RetryBackoffPolicy } from "../shared/backoff.js";
import { describeError } from "../shared/errors.js";
import { abortableSleep } from "../shared/sleep.js";
import { PUBLISH_TIMEOUT_GRACE_MS } from "./outbox.config.js";
import {
  claimPublishableEvents,
  markEventsPublished,
  recordPublicationFailures,
  type ClaimedOutboxEvent,
  type PublicationFailure,
} from "./outbox.store.js";
import type { DeliveryReceipt, MessageTransport, OutboundMessage } from "./outbox.transport.js";

// Log messages are part of the publisher's operational contract: tests match
// on them, and so may log-based alerts.
export const OUTBOX_LOG_MESSAGES = {
  published: "outbox event published",
  publicationFailed: "outbox event publication failed; it will be retried",
  unpublishable:
    "outbox event cannot be published; it blocks later events for its auction until repaired",
  lateAcknowledgement:
    "Kafka acknowledged an outbox event after its publication timed out; a duplicate will follow",
  markerNotCommitted:
    "Kafka acknowledged outbox events whose publication marker was not committed; they will be published again",
  markerCommitUnknown:
    "Kafka acknowledged outbox events but the commit outcome of their publication marker is unknown; they may be published again",
  cycleFailed: "outbox publication cycle failed",
} as const;

// The publication contract for each event type. An event type without an
// entry cannot be published.
const payloadValidators = new Map<string, (payload: unknown) => unknown>([
  [BID_ACCEPTED_V1_EVENT_TYPE, parseBidAcceptedV1],
  [AUCTION_CLOSED_V1_EVENT_TYPE, parseAuctionClosedV1],
]);

export interface OutboxPublisherOptions {
  database: Database;
  transport: MessageTransport;
  topic: string;
  batchSize: number;
  pollIntervalMs: number;
  publishTimeoutMs: number;
  retryBackoff: RetryBackoffPolicy;
  logger: Logger;
  random?: () => number;
}

export interface PublishBatchResult {
  claimed: number;
  published: number;
  failed: number;
}

type PublicationOutcome =
  | { kind: "published"; event: ClaimedOutboxEvent; receipt: DeliveryReceipt }
  | { kind: "failed"; failure: PublicationFailure };

type PublishedOutcome = Extract<PublicationOutcome, { kind: "published" }>;

class UnpublishableEventError extends Error {
  override readonly name = "UnpublishableEventError";
}

class PublishTimeoutError extends Error {
  override readonly name = "PublishTimeoutError";
}

export class OutboxPublisher {
  private readonly random: () => number;

  constructor(private readonly options: OutboxPublisherOptions) {
    this.random = options.random ?? Math.random;
  }

  // One claim → publish → record → commit cycle in a single transaction.
  //
  // The claimed rows stay locked while Kafka is contacted, and published_at
  // is written only for events whose send Kafka acknowledged. If this process
  // dies, or the transaction fails, after an acknowledgement but before
  // COMMIT is sent, the rows are unlocked still unpublished and the same
  // stored event is sent again later: delivery is at-least-once. If COMMIT
  // itself reports an error, its outcome is unknown: the reply may have been
  // lost after PostgreSQL committed, in which case nothing is sent again.
  async publishBatch(): Promise<PublishBatchResult> {
    const client = await this.options.database.connect();
    let cycleError: unknown;
    let acknowledged: ClaimedOutboxEvent[] = [];
    let commitSent = false;

    try {
      await client.query("BEGIN");
      const events = await claimPublishableEvents(client, this.options.batchSize);
      if (events.length === 0) {
        await client.query("COMMIT");
        return { claimed: 0, published: 0, failed: 0 };
      }

      const outcomes = await Promise.all(events.map(async (event) => this.publishOne(event)));
      const published = outcomes.filter((outcome) => outcome.kind === "published");
      const failures = outcomes.flatMap((outcome) =>
        outcome.kind === "failed" ? [outcome.failure] : [],
      );
      acknowledged = published.map((outcome) => outcome.event);

      await markEventsPublished(
        client,
        acknowledged.map((event) => event.eventId),
      );
      await recordPublicationFailures(client, failures);
      commitSent = true;
      await client.query("COMMIT");

      this.logPublished(published);
      return { claimed: events.length, published: published.length, failed: failures.length };
    } catch (error) {
      cycleError = error;
      await rollbackQuietly(client);
      this.warnAboutUncommittedAcknowledgements(error, acknowledged, commitSent);
      throw error;
    } finally {
      // Never return a connection in an unknown transaction state to the
      // pool: pg destroys the client when release receives an error.
      client.release(cycleError === undefined ? undefined : toError(cycleError));
    }
  }

  // Runs publication cycles until the signal aborts. An in-flight cycle is
  // always allowed to finish (it is bounded by the publish timeout) so that
  // acknowledged events are marked instead of being republished.
  async run(signal: AbortSignal): Promise<void> {
    let consecutiveFailures = 0;

    while (!signal.aborted) {
      let pauseMs: number;
      try {
        const result = await this.publishBatch();
        consecutiveFailures = 0;
        // Continue immediately only while a full batch went out cleanly;
        // otherwise there is either nothing to do or something is failing.
        const fullCleanBatch = result.claimed === this.options.batchSize && result.failed === 0;
        pauseMs = fullCleanBatch ? 0 : this.options.pollIntervalMs;
      } catch (error) {
        consecutiveFailures += 1;
        pauseMs = backoffDelayMs(this.options.retryBackoff, consecutiveFailures, this.random);
        this.options.logger.error(
          { err: error, consecutiveFailures, retryDelayMs: pauseMs },
          OUTBOX_LOG_MESSAGES.cycleFailed,
        );
      }

      if (pauseMs > 0) {
        await abortableSleep(pauseMs, signal);
      }
    }
  }

  private async publishOne(event: ClaimedOutboxEvent): Promise<PublicationOutcome> {
    try {
      this.assertPublishable(event);
      const receipt = await this.sendWithTimeout(event);
      return { kind: "published", event, receipt };
    } catch (error) {
      const attempt = event.publishAttempts + 1;
      const retryDelayMs = backoffDelayMs(this.options.retryBackoff, attempt, this.random);
      this.options.logger.error(
        { err: error, ...eventLogFields(event), attempt, retryDelayMs },
        error instanceof UnpublishableEventError
          ? OUTBOX_LOG_MESSAGES.unpublishable
          : OUTBOX_LOG_MESSAGES.publicationFailed,
      );
      return {
        kind: "failed",
        failure: { eventId: event.eventId, retryDelayMs, error: describeError(error) },
      };
    }
  }

  private assertPublishable(event: ClaimedOutboxEvent): void {
    const validate = payloadValidators.get(event.eventType);
    if (validate === undefined) {
      throw new UnpublishableEventError(`No publication contract for ${event.eventType}`);
    }
    try {
      validate(JSON.parse(event.payloadJson));
    } catch (error) {
      throw new UnpublishableEventError(
        `Stored payload does not satisfy ${event.eventType}: ${describeError(error)}`,
      );
    }
  }

  // Races the send against the publisher's own timer. librdkafka enforces the
  // publish timeout itself, but only approximately; the timer bounds a send
  // that never settles.
  private async sendWithTimeout(event: ClaimedOutboxEvent): Promise<DeliveryReceipt> {
    const sending = this.options.transport.send(this.toMessage(event));
    try {
      return await withTimeout(sending, this.options.publishTimeoutMs + PUBLISH_TIMEOUT_GRACE_MS);
    } catch (error) {
      if (error instanceof PublishTimeoutError) {
        this.watchLateDelivery(event, sending);
      }
      throw error;
    }
  }

  // A send abandoned by the publisher's timer may still be delivered. The
  // event is recorded as failed and will be sent again, so this produces a
  // late duplicate that consumers must tolerate.
  private watchLateDelivery(event: ClaimedOutboxEvent, sending: Promise<DeliveryReceipt>): void {
    sending.then(
      (receipt) =>
        this.options.logger.warn(
          { ...eventLogFields(event), partition: receipt.partition, offset: receipt.offset },
          OUTBOX_LOG_MESSAGES.lateAcknowledgement,
        ),
      () => {},
    );
  }

  // The message value is the stored payload text itself, not a re-encoding of
  // a domain object, so event ID and content are identical on every attempt.
  private toMessage(event: ClaimedOutboxEvent): OutboundMessage {
    return {
      topic: this.options.topic,
      key: event.auctionId,
      value: event.payloadJson,
      headers: {
        [EVENT_ID_HEADER]: event.eventId,
        [EVENT_TYPE_HEADER]: event.eventType,
        "content-type": "application/json",
      },
    };
  }

  private logPublished(published: readonly PublishedOutcome[]): void {
    for (const { event, receipt } of published) {
      this.options.logger.debug(
        {
          ...eventLogFields(event),
          partition: receipt.partition,
          offset: receipt.offset,
          attempt: event.publishAttempts + 1,
        },
        OUTBOX_LOG_MESSAGES.published,
      );
    }
  }

  private warnAboutUncommittedAcknowledgements(
    error: unknown,
    acknowledged: readonly ClaimedOutboxEvent[],
    commitSent: boolean,
  ): void {
    if (acknowledged.length === 0) {
      return;
    }
    this.options.logger.warn(
      { err: error, eventIds: acknowledged.map((event) => event.eventId) },
      commitSent ? OUTBOX_LOG_MESSAGES.markerCommitUnknown : OUTBOX_LOG_MESSAGES.markerNotCommitted,
    );
  }
}

function eventLogFields(event: ClaimedOutboxEvent): Record<string, string> {
  return {
    eventId: event.eventId,
    eventType: event.eventType,
    auctionId: event.auctionId,
    auctionVersion: event.auctionVersion,
  };
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new PublishTimeoutError(`Kafka did not acknowledge within ${timeoutMs}ms`)),
      timeoutMs,
    );
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}
