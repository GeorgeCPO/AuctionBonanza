// Copied from auction-service's test support: tests keep no code in common.

import { setTimeout as delay } from "node:timers/promises";

import type { PartitionOffsets, SettledOffsetsOptions } from "./topic-offsets.types.js";

// Partitions of a freshly created topic can answer an offsets query with -1,
// "not available yet", instead of an error. Summing high - low over such an
// answer subtracts from the real count: a partition reporting high -1 and
// low 0 cancels the one message held by another partition, and the topic looks
// empty. Only an answer in which every partition reports real offsets counts.
export function isSettled(offsets: readonly PartitionOffsets[]): boolean {
  return (
    offsets.length > 0 &&
    offsets.every((partition) => BigInt(partition.high) >= 0n && BigInt(partition.low) >= 0n)
  );
}

// The number of messages currently held, from a settled answer.
export function countMessages(offsets: readonly PartitionOffsets[]): number {
  return offsets.reduce(
    (sum, partition) => sum + Number(BigInt(partition.high) - BigInt(partition.low)),
    0,
  );
}

// Asks again until the answer is settled. The unavailable offsets clear within
// a few polls, so a topic that never settles is reported as an error rather
// than read as empty.
export async function fetchSettledOffsets<Offsets extends PartitionOffsets>(
  fetchOffsets: () => Promise<readonly Offsets[]>,
  options: SettledOffsetsOptions,
): Promise<readonly Offsets[]> {
  const deadline = Date.now() + options.timeoutMs;
  for (;;) {
    const offsets = await fetchOffsets();
    if (isSettled(offsets)) {
      return offsets;
    }
    if (Date.now() > deadline) {
      throw new Error(
        `Offsets of ${options.topic} were still unavailable after ${options.timeoutMs}ms: ${JSON.stringify(offsets)}`,
      );
    }
    await delay(options.pollMs);
  }
}
