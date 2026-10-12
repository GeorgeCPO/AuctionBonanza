import { describe, expect, it } from "vitest";

import { countMessages, fetchSettledOffsets, isSettled } from "../support/topic-offsets.js";
import type { PartitionOffsets } from "../support/topic-offsets.types.js";

const unavailable: PartitionOffsets = { high: "-1", low: "-1" };
const empty: PartitionOffsets = { high: "0", low: "0" };
const oneMessage: PartitionOffsets = { high: "1", low: "0" };

// Captured from a broker right after a message was acknowledged on a fresh
// six-partition topic: partition 4 holds the message, partition 5 reports its
// high watermark as unavailable but its log start as 0.
const answerBeforeSettling: PartitionOffsets[] = [
  unavailable,
  unavailable,
  unavailable,
  empty,
  oneMessage,
  { high: "-1", low: "0" },
];

const options = { topic: "orders", timeoutMs: 1_000, pollMs: 1 };

describe("isSettled", () => {
  it("accepts an answer in which every partition reports real offsets", () => {
    expect(isSettled([empty, oneMessage, { high: "7", low: "3" }])).toBe(true);
  });

  it("rejects an answer in which any high or low watermark is unavailable", () => {
    expect(isSettled([empty, unavailable])).toBe(false);
    expect(isSettled([empty, { high: "-1", low: "0" }])).toBe(false);
    expect(isSettled([empty, { high: "0", low: "-1" }])).toBe(false);
  });

  it("rejects an answer that lists no partitions", () => {
    expect(isSettled([])).toBe(false);
  });
});

describe("countMessages", () => {
  it("sums the retained messages of every partition", () => {
    expect(countMessages([empty, oneMessage, { high: "7", low: "3" }])).toBe(5);
  });

  it("is wrong for an unsettled answer, which is why it must not be given one", () => {
    // One message is held, but the unavailable partition subtracts it again.
    expect(countMessages(answerBeforeSettling)).toBe(0);
    expect(isSettled(answerBeforeSettling)).toBe(false);
  });
});

describe("fetchSettledOffsets", () => {
  it("asks again until the answer settles and returns that answer", async () => {
    const settled = [empty, oneMessage, empty];
    const answers = [answerBeforeSettling, [unavailable, oneMessage, empty], settled];
    let asked = 0;

    const offsets = await fetchSettledOffsets(async () => answers[asked++]!, options);

    expect(offsets).toBe(settled);
    expect(asked).toBe(3);
    expect(countMessages(offsets)).toBe(1);
  });

  it("asks only once when the first answer is already settled", async () => {
    let asked = 0;

    await fetchSettledOffsets(async () => {
      asked += 1;
      return [empty, empty];
    }, options);

    expect(asked).toBe(1);
  });

  it("reports a topic that never settles instead of reading it as empty", async () => {
    await expect(
      fetchSettledOffsets(async () => answerBeforeSettling, { ...options, timeoutMs: 30 }),
    ).rejects.toThrow(/Offsets of orders were still unavailable after 30ms/);
  });

  it("passes a failure of the query through", async () => {
    await expect(
      fetchSettledOffsets(async () => {
        throw new Error("broker unreachable");
      }, options),
    ).rejects.toThrow("broker unreachable");
  });
});
