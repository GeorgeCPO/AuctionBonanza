import type { HeaderValue } from "./consumed-record.types.js";
import type {
  BuildDeadLetterInput,
  DeadLetterHeaders,
  DeadLetterRecord,
} from "./dead-letter.types.js";

// Headers added to every dead-lettered record. They overwrite a header of the
// same name on the source record, which only happens when a record that was
// itself dead-lettered earlier is produced to auction.events again.
export const DEAD_LETTER_HEADERS = {
  reason: "dlq-reason",
  detail: "dlq-detail",
  sourceTopic: "dlq-source-topic",
  sourcePartition: "dlq-source-partition",
  sourceOffset: "dlq-source-offset",
  consumerGroup: "dlq-consumer-group",
  failedAt: "dlq-failed-at",
} as const;

// A cause can quote a database error. Keep the header to a sensible size.
const MAX_DETAIL_LENGTH = 1_000;

// Builds the record for the dead-letter topic. Pure: the key, value and own
// headers are carried over untouched, so a person repairing the record sees
// exactly what the producer sent, and the dlq-* headers say where it came from
// (topic, partition and offset make a duplicate dead letter recognisable) and
// why it is here.
export function buildDeadLetterRecord(input: BuildDeadLetterInput): DeadLetterRecord {
  const { record, cause } = input;
  const headers: Record<string, HeaderValue | readonly HeaderValue[]> = {};
  for (const [name, value] of Object.entries(record.headers)) {
    if (value !== undefined) {
      headers[name] = value;
    }
  }

  const added: DeadLetterHeaders = {
    [DEAD_LETTER_HEADERS.reason]: cause.reason,
    [DEAD_LETTER_HEADERS.detail]: cause.detail.slice(0, MAX_DETAIL_LENGTH),
    [DEAD_LETTER_HEADERS.sourceTopic]: record.topic,
    [DEAD_LETTER_HEADERS.sourcePartition]: String(record.partition),
    [DEAD_LETTER_HEADERS.sourceOffset]: record.offset,
    [DEAD_LETTER_HEADERS.consumerGroup]: input.consumerGroup,
    [DEAD_LETTER_HEADERS.failedAt]: input.failedAt.toISOString(),
  };

  return { key: record.key, value: record.value, headers: { ...headers, ...added } };
}
