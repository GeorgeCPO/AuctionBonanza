// The part of the Kafka admin client's per-partition offsets that the tests
// read. Both are decimal strings; -1 means the broker has not made the offset
// available yet.
export interface PartitionOffsets {
  readonly high: string;
  readonly low: string;
}

export interface SettledOffsetsOptions {
  readonly topic: string;
  readonly timeoutMs: number;
  readonly pollMs: number;
}
