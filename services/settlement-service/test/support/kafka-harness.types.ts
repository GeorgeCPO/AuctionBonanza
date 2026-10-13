export interface TestRecord {
  key: string | Buffer | null;
  value: string | Buffer | null;
  // Sent as given: leave a header out to test its absence.
  headers?: Record<string, string>;
}

export interface ProducedRecord {
  partition: number;
  offset: bigint;
}

export interface ReadRecord {
  partition: number;
  offset: bigint;
  key: Buffer | null;
  value: Buffer | null;
  headers: Record<string, string>;
}
