// The same policy as auction-service's outbox publisher and the realtime
// relay, deliberately copied rather than shared: it is small, and services
// keep no common runtime code.

export interface RetryBackoffPolicy {
  baseDelayMs: number;
  maxDelayMs: number;
}

// Capped exponential backoff with "equal jitter": the delay for attempt n is
// uniformly drawn from [ceiling / 2, ceiling], where
// ceiling = min(maxDelayMs, baseDelayMs * 2^(n - 1)). Keeping half of the
// ceiling fixed guarantees real spacing between attempts; the random half
// spreads out retries of messages that failed together during one outage.
export function backoffDelayMs(
  policy: RetryBackoffPolicy,
  attempt: number,
  random: () => number = Math.random,
): number {
  if (!Number.isInteger(attempt) || attempt < 1) {
    throw new RangeError("attempt must be a positive integer");
  }
  // Clamp the exponent so large attempt counts cannot overflow to Infinity.
  const exponent = Math.min(attempt - 1, 30);
  const ceiling = Math.min(policy.maxDelayMs, policy.baseDelayMs * 2 ** exponent);
  const half = Math.floor(ceiling / 2);
  return ceiling - half + Math.floor(random() * (half + 1));
}
