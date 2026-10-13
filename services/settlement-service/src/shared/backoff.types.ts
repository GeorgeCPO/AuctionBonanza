export interface RetryBackoffPolicy {
  baseDelayMs: number;
  maxDelayMs: number;
}
