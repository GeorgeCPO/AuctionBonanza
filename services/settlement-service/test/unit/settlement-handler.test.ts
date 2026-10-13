import { beforeEach, describe, expect, it } from "vitest";

import { DEAD_LETTER_HEADERS } from "../../src/dead-letter.record.js";
import { PermanentSettlementError } from "../../src/settlement.errors.js";
import { SettlementHandler } from "../../src/settlement.handler.js";
import { SETTLEMENT_LOG_MESSAGES } from "../../src/settlement.log-messages.js";
import { bidAcceptedPayload, recordOf, soldEvent, unsoldEvent } from "../support/events.js";
import { RecordingDeadLetters, ScriptedRepository } from "../support/handler-fakes.js";
import { captureLogger } from "../support/log-capture.js";

describe("SettlementHandler", () => {
  let repository: ScriptedRepository;
  let deadLetters: RecordingDeadLetters;
  let log: ReturnType<typeof captureLogger>;
  let handler: SettlementHandler;

  beforeEach(() => {
    repository = new ScriptedRepository();
    deadLetters = new RecordingDeadLetters();
    log = captureLogger();
    handler = new SettlementHandler({
      repository,
      deadLetters,
      consumerGroup: "settlement-worker",
      logger: log.logger,
      now: () => new Date("2030-01-02T03:04:05.678Z"),
    });
  });

  function messages(): string[] {
    return log.lines.map((line) => line.msg as string);
  }

  describe("an AuctionClosed.v1", () => {
    it("settles a sold auction and reports the order", async () => {
      const event = soldEvent();
      repository.outcome = () => ({ kind: "order_created", orderId: "77" });

      const outcome = await handler.handle(recordOf(event));

      expect(outcome).toEqual({
        kind: "settled",
        outcome: { kind: "order_created", orderId: "77" },
      });
      expect(repository.settled).toEqual([event]);
      expect(deadLetters.published).toEqual([]);
      expect(log.lines).toContainEqual(
        expect.objectContaining({
          msg: SETTLEMENT_LOG_MESSAGES.orderCreated,
          orderId: "77",
          auctionId: event.auctionId,
          eventId: event.eventId,
        }),
      );
    });

    it("settles an unsold auction, which creates no order", async () => {
      repository.outcome = () => ({ kind: "unsold_recorded" });

      const outcome = await handler.handle(recordOf(unsoldEvent()));

      expect(outcome).toEqual({ kind: "settled", outcome: { kind: "unsold_recorded" } });
      expect(messages()).toContain(SETTLEMENT_LOG_MESSAGES.unsoldRecorded);
    });

    it("acknowledges a repeat delivery without a second effect", async () => {
      repository.outcome = () => ({ kind: "duplicate" });

      const outcome = await handler.handle(recordOf(soldEvent()));

      expect(outcome).toEqual({ kind: "settled", outcome: { kind: "duplicate" } });
      expect(deadLetters.published).toEqual([]);
      // Debug, not info: replaying history repeats every close already settled.
      expect(log.lines).toContainEqual(
        expect.objectContaining({ level: 20, msg: SETTLEMENT_LOG_MESSAGES.duplicate }),
      );
    });
  });

  describe("an event settlement does not own", () => {
    it("is ignored without touching the database or the dead-letter topic", async () => {
      const payload = bidAcceptedPayload("42", 4);
      const record = {
        ...recordOf(soldEvent(), { eventType: "BidAccepted.v1", value: JSON.stringify(payload) }),
      };

      const outcome = await handler.handle(record);

      expect(outcome).toEqual({ kind: "ignored", eventType: "BidAccepted.v1" });
      expect(repository.settled).toEqual([]);
      expect(deadLetters.published).toEqual([]);
      // Bids are expected, so they are noted only at debug level.
      expect(log.lines).toContainEqual(
        expect.objectContaining({ level: 20, msg: SETTLEMENT_LOG_MESSAGES.ignored }),
      );
    });

    it("warns about a type it does not know, because a close under a new name would be lost", async () => {
      const event = soldEvent();
      const record = recordOf(event, { eventType: "AuctionClosed.v2" });

      const outcome = await handler.handle(record);

      expect(outcome).toEqual({ kind: "ignored", eventType: "AuctionClosed.v2" });
      expect(repository.settled).toEqual([]);
      expect(deadLetters.published).toEqual([]);
      expect(log.lines).toContainEqual(
        expect.objectContaining({
          level: 40,
          msg: SETTLEMENT_LOG_MESSAGES.unknownEventType,
          eventType: "AuctionClosed.v2",
        }),
      );
    });
  });

  describe("a record that can never be settled", () => {
    it("is sent to the dead-letter topic unchanged, and acknowledged", async () => {
      const record = recordOf(soldEvent(), { value: "{not json", offset: "123" });

      const outcome = await handler.handle(record);

      expect(outcome).toEqual({ kind: "dead_lettered", reason: "invalid-json" });
      expect(repository.settled).toEqual([]);
      expect(deadLetters.published).toHaveLength(1);
      const [deadLetter] = deadLetters.published;
      expect(deadLetter?.key).toBe(record.key);
      expect(deadLetter?.value).toBe(record.value);
      expect(deadLetter?.headers).toMatchObject({
        [DEAD_LETTER_HEADERS.reason]: "invalid-json",
        [DEAD_LETTER_HEADERS.sourceOffset]: "123",
        [DEAD_LETTER_HEADERS.consumerGroup]: "settlement-worker",
        [DEAD_LETTER_HEADERS.failedAt]: "2030-01-02T03:04:05.678Z",
      });
      expect(log.lines).toContainEqual(
        expect.objectContaining({
          level: 50,
          msg: SETTLEMENT_LOG_MESSAGES.deadLettered,
          reason: "invalid-json",
          offset: "123",
        }),
      );
    });

    it("is sent to the dead-letter topic when the database refuses its data", async () => {
      repository.outcome = () => {
        throw new PermanentSettlementError("order does not match the winning bid", {
          sqlState: "23514",
          constraint: "orders_terms_match_winning_bid",
        });
      };

      const outcome = await handler.handle(recordOf(soldEvent()));

      expect(outcome).toEqual({ kind: "dead_lettered", reason: "database-rejected" });
      expect(deadLetters.published).toHaveLength(1);
      expect(deadLetters.published[0]?.headers).toMatchObject({
        [DEAD_LETTER_HEADERS.reason]: "database-rejected",
        [DEAD_LETTER_HEADERS.detail]:
          "PermanentSettlementError: order does not match the winning bid " +
          "[SQLSTATE 23514, constraint orders_terms_match_winning_bid]",
      });
    });
  });

  describe("a database refusal that names no SQLSTATE", () => {
    it.each([
      [
        "a constraint",
        { sqlState: undefined, constraint: "settlement_unsold_matches_auction" },
        "PermanentSettlementError: contradicted [constraint settlement_unsold_matches_auction]",
      ],
      [
        "nothing",
        { sqlState: undefined, constraint: undefined },
        "PermanentSettlementError: contradicted",
      ],
    ])("describes it by %s without inventing one", async (_description, details, expected) => {
      repository.outcome = () => {
        throw new PermanentSettlementError("contradicted", details);
      };

      await handler.handle(recordOf(soldEvent()));

      expect(deadLetters.published[0]?.headers).toMatchObject({
        [DEAD_LETTER_HEADERS.reason]: "database-rejected",
        [DEAD_LETTER_HEADERS.detail]: expected,
      });
    });
  });

  describe("a failure that may clear", () => {
    it("rethrows a database error, so the record is retried and not dead-lettered", async () => {
      const outage = new Error("connection terminated unexpectedly");
      repository.outcome = () => {
        throw outage;
      };

      await expect(handler.handle(recordOf(soldEvent()))).rejects.toBe(outage);

      expect(deadLetters.published).toEqual([]);
      expect(messages()).not.toContain(SETTLEMENT_LOG_MESSAGES.orderCreated);
    });

    it("rethrows when the dead-letter topic fails for a record the database refused", async () => {
      repository.outcome = () => {
        throw new PermanentSettlementError("refused", { sqlState: "23505", constraint: undefined });
      };
      const outage = new Error("broker unavailable");
      deadLetters.failWith = outage;

      await expect(handler.handle(recordOf(soldEvent()))).rejects.toBe(outage);
    });

    it("holds a record that cannot be read while the dead-letter topic is down, and dead-letters it once the topic is back", async () => {
      const record = recordOf(soldEvent(), { key: "43" });
      const outage = new Error("broker unavailable");
      deadLetters.failWith = outage;
      await expect(handler.handle(record)).rejects.toBe(outage);
      expect(messages()).not.toContain(SETTLEMENT_LOG_MESSAGES.deadLettered);

      deadLetters.failWith = undefined;
      const outcome = await handler.handle(record);

      expect(outcome).toEqual({ kind: "dead_lettered", reason: "key-mismatch" });
      expect(deadLetters.published).toHaveLength(1);
    });
  });

  describe("the record's headers", () => {
    it("accepts header values delivered as strings or as lists", async () => {
      const event = soldEvent();
      const record = {
        ...recordOf(event),
        headers: {
          "event-type": [event.eventType, "ignored second value"],
          "event-id": event.eventId,
        },
      };

      const outcome = await handler.handle(record);

      expect(outcome.kind).toBe("settled");
    });
  });
});
