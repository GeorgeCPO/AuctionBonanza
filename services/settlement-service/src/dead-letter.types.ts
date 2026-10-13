import type { ConsumedRecord, HeaderValue } from "./consumed-record.types.js";

export type DeadLetterHeaders = Readonly<Record<string, HeaderValue | readonly HeaderValue[]>>;

// What is produced to the dead-letter topic: the source record's key and value
// exactly as consumed, its own headers, and the dlq-* headers explaining why it
// is there.
export interface DeadLetterRecord {
  readonly key: Buffer | null;
  readonly value: Buffer | null;
  readonly headers: DeadLetterHeaders;
}

export interface DeadLetterCause {
  // A short, stable label (a RejectionReason or "database-rejected"), so
  // dead-lettered records can be grouped and counted.
  readonly reason: string;
  readonly detail: string;
}

export interface BuildDeadLetterInput {
  readonly record: ConsumedRecord;
  readonly cause: DeadLetterCause;
  readonly consumerGroup: string;
  readonly failedAt: Date;
}

export interface DeadLetterPublisher {
  // Resolves only after the broker acknowledged the record, and rejects on any
  // failure or timeout. The caller treats a rejection as transient and
  // retries, so a record may be dead-lettered more than once (at-least-once).
  publish(record: DeadLetterRecord): Promise<void>;
}
