export type HeaderValue = Buffer | string;

export type RecordHeaders = Readonly<
  Record<string, HeaderValue | readonly HeaderValue[] | undefined>
>;

// One auction.events record as the Kafka client delivered it. The key, value
// and headers stay as bytes, untouched, so a record that has to be
// dead-lettered is copied exactly: a person repairing it needs what the
// producer actually sent, not our reading of it.
export interface ConsumedRecord {
  readonly topic: string;
  readonly partition: number;
  // A decimal string: offsets can exceed what a JavaScript number holds.
  readonly offset: string;
  readonly key: Buffer | null;
  readonly value: Buffer | null;
  readonly headers: RecordHeaders;
}
