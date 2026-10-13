import { describe, expect, it } from "vitest";

import { decideSettlement } from "../../src/settlement.decision.js";
import type { DecisionInput } from "../../src/settlement.decision.types.js";
import { bidAcceptedPayload, soldEvent, unsoldEvent } from "../support/events.js";

// The input the outbox publisher's messages give: key auctionId, headers
// event-type and event-id, value the event as JSON.
function inputFor(
  event: object & { eventId?: string; auctionId?: string; eventType?: string },
  overrides: Partial<DecisionInput> = {},
): DecisionInput {
  return {
    key: event.auctionId ?? null,
    value: JSON.stringify(event),
    eventType: event.eventType,
    eventId: event.eventId,
    ...overrides,
  };
}

describe("decideSettlement", () => {
  describe("settles", () => {
    it("a sold auction", () => {
      const event = soldEvent();

      expect(decideSettlement(inputFor(event))).toEqual({ kind: "settle", event });
    });

    it("an unsold auction", () => {
      const event = unsoldEvent();

      expect(decideSettlement(inputFor(event))).toEqual({ kind: "settle", event });
    });
  });

  describe("ignores", () => {
    it("an event type settlement does not own", () => {
      const payload = bidAcceptedPayload("42", 3) as { eventId: string; eventType: string };

      expect(decideSettlement(inputFor(payload, { key: "42" }))).toEqual({
        kind: "ignore",
        eventType: "BidAccepted.v1",
      });
    });

    it("an unknown event type, without reading its value", () => {
      expect(
        decideSettlement({
          key: null,
          value: null,
          eventType: "AuctionCancelled.v1",
          eventId: undefined,
        }),
      ).toEqual({ kind: "ignore", eventType: "AuctionCancelled.v1" });
    });
  });

  describe("rejects", () => {
    it("a record without an event-type header", () => {
      const event = soldEvent();

      expect(decideSettlement(inputFor(event, { eventType: undefined }))).toEqual({
        kind: "reject",
        reason: "missing-event-type",
        detail: "missing event-type header",
      });
    });

    it("an AuctionClosed.v1 without a value", () => {
      const event = soldEvent();

      expect(decideSettlement(inputFor(event, { value: null }))).toMatchObject({
        kind: "reject",
        reason: "missing-value",
      });
    });

    it("a value that is not JSON", () => {
      const event = soldEvent();

      expect(decideSettlement(inputFor(event, { value: "{not json" }))).toMatchObject({
        kind: "reject",
        reason: "invalid-json",
      });
    });

    it.each([
      ["a missing field", { ...soldEvent(), currencyCode: undefined }],
      ["an extra field", { ...soldEvent(), sellerId: "7" }],
      ["a malformed id", { ...soldEvent(), auctionId: "0042" }],
      ["a malformed currency", { ...soldEvent(), currencyCode: "usd" }],
      ["an invalid timestamp", { ...soldEvent(), occurredAt: "2030-02-31T10:00:00.000000Z" }],
      ["a winner without a price", { ...soldEvent(), finalAmountMinor: null }],
      ["a price without a winner", { ...unsoldEvent(), finalAmountMinor: "100" }],
      ["a payload that is not an object", "just a string"],
    ])("a payload with %s", (_description, payload) => {
      const event = soldEvent();

      const decision = decideSettlement(inputFor(event, { value: JSON.stringify(payload) }));

      expect(decision).toMatchObject({ kind: "reject", reason: "contract-violation" });
    });

    it("quotes the schema's first complaint for a payload that breaks the schema", () => {
      const event = soldEvent();

      const decision = decideSettlement(
        inputFor(event, { value: JSON.stringify({ ...event, auctionId: "0042" }) }),
      );

      expect(decision).toMatchObject({ detail: expect.stringContaining("/auctionId") });
    });

    it("says so when only the contract's own rules are broken", () => {
      const event = soldEvent();

      const decision = decideSettlement(
        inputFor(event, { value: JSON.stringify({ ...event, finalAmountMinor: null }) }),
      );

      expect(decision).toMatchObject({
        detail: "payload satisfies the schema but not the AuctionClosed.v1 contract",
      });
    });

    it.each([
      ["another auction's key", "43"],
      ["no key", null],
    ])("a record with %s", (_description, key) => {
      const event = soldEvent();

      expect(decideSettlement(inputFor(event, { key }))).toMatchObject({
        kind: "reject",
        reason: "key-mismatch",
      });
    });

    it.each([
      ["another event's id", "bb106586-ded1-4eca-9a75-1a38c170499f"],
      ["no event-id header", undefined],
    ])("a record with %s in its header", (_description, eventId) => {
      const event = soldEvent();

      expect(decideSettlement(inputFor(event, { eventId }))).toMatchObject({
        kind: "reject",
        reason: "event-id-mismatch",
      });
    });
  });
});
