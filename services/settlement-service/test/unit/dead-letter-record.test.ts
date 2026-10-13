import { describe, expect, it } from "vitest";

import { buildDeadLetterRecord, DEAD_LETTER_HEADERS } from "../../src/dead-letter.record.js";
import { recordOf, soldEvent } from "../support/events.js";

const failedAt = new Date("2030-01-02T03:04:05.678Z");

describe("buildDeadLetterRecord", () => {
  it("carries the key and value over byte for byte", () => {
    const record = {
      ...recordOf(soldEvent()),
      key: Buffer.from([0xff, 0x00, 0x7f]),
      value: Buffer.from([0xc3, 0x28, 0x00, 0xa0]),
    };

    const deadLetter = buildDeadLetterRecord({
      record,
      cause: { reason: "invalid-json", detail: "bad bytes" },
      consumerGroup: "settlement-worker",
      failedAt,
    });

    expect(deadLetter.key).toBe(record.key);
    expect(deadLetter.value).toBe(record.value);
  });

  it("keeps a missing key and a missing value missing", () => {
    const record = { ...recordOf(soldEvent()), key: null, value: null };

    const deadLetter = buildDeadLetterRecord({
      record,
      cause: { reason: "missing-value", detail: "missing value" },
      consumerGroup: "settlement-worker",
      failedAt,
    });

    expect(deadLetter.key).toBeNull();
    expect(deadLetter.value).toBeNull();
  });

  it("keeps the source record's own headers", () => {
    const event = soldEvent();
    const record = {
      ...recordOf(event),
      headers: {
        ...recordOf(event).headers,
        repeated: [Buffer.from("a"), "b"],
        absent: undefined,
      },
    };

    const { headers } = buildDeadLetterRecord({
      record,
      cause: { reason: "key-mismatch", detail: "d" },
      consumerGroup: "settlement-worker",
      failedAt,
    });

    expect(headers["event-type"]).toEqual(Buffer.from("AuctionClosed.v1"));
    expect(headers["event-id"]).toEqual(Buffer.from(event.eventId));
    expect(headers["repeated"]).toEqual([Buffer.from("a"), "b"]);
    expect(headers).not.toHaveProperty("absent");
  });

  it("says why, where from, and when", () => {
    const record = recordOf(soldEvent(), { partition: 5, offset: "9007199254740993" });

    const { headers } = buildDeadLetterRecord({
      record,
      cause: { reason: "database-rejected", detail: "PermanentSettlementError: no" },
      consumerGroup: "settlement-worker",
      failedAt,
    });

    expect(headers).toMatchObject({
      [DEAD_LETTER_HEADERS.reason]: "database-rejected",
      [DEAD_LETTER_HEADERS.detail]: "PermanentSettlementError: no",
      [DEAD_LETTER_HEADERS.sourceTopic]: "auction.events",
      [DEAD_LETTER_HEADERS.sourcePartition]: "5",
      // Past Number.MAX_SAFE_INTEGER: the offset stays a string end to end.
      [DEAD_LETTER_HEADERS.sourceOffset]: "9007199254740993",
      [DEAD_LETTER_HEADERS.consumerGroup]: "settlement-worker",
      [DEAD_LETTER_HEADERS.failedAt]: "2030-01-02T03:04:05.678Z",
    });
  });

  it("cuts a long detail to a sensible size", () => {
    const { headers } = buildDeadLetterRecord({
      record: recordOf(soldEvent()),
      cause: { reason: "database-rejected", detail: "x".repeat(5_000) },
      consumerGroup: "settlement-worker",
      failedAt,
    });

    expect(headers[DEAD_LETTER_HEADERS.detail]).toHaveLength(1_000);
  });

  it("overwrites dlq headers left on a record that was dead-lettered before", () => {
    const record = recordOf(soldEvent());
    const replayed = {
      ...record,
      headers: { ...record.headers, [DEAD_LETTER_HEADERS.reason]: "old-reason" },
    };

    const { headers } = buildDeadLetterRecord({
      record: replayed,
      cause: { reason: "new-reason", detail: "d" },
      consumerGroup: "settlement-worker",
      failedAt,
    });

    expect(headers[DEAD_LETTER_HEADERS.reason]).toBe("new-reason");
  });
});
