import { describe, expect, it } from "vitest";

import { decideRelay } from "../../src/relay/relay.decision.js";
import { bidAccepted } from "../support/relay-harness.js";

function record(auctionId = "42", version = 7) {
  const { key, value, eventType } = bidAccepted(auctionId, version);
  return { key, value, eventType };
}

describe("decideRelay", () => {
  it("publishes a valid event to its auction's channel inside a versioned envelope", () => {
    const input = record("42", 7);

    const decision = decideRelay(input);

    expect(decision).toMatchObject({ kind: "publish", channel: "auction:42" });
    if (decision.kind !== "publish") {
      throw new Error("expected a publish");
    }
    expect(JSON.parse(decision.message)).toEqual({
      v: 1,
      eventType: "BidAccepted.v1",
      event: JSON.parse(input.value!),
    });
  });

  it("publishes an AuctionClosed.v1 event, sold or unsold, to its auction's channel", () => {
    const closed = {
      eventId: "bb106586-ded1-4eca-9a75-1a38c170499f",
      eventType: "AuctionClosed.v1",
      occurredAt: "2026-10-12T09:15:30.123456Z",
      auctionId: "42",
      auctionVersion: "8",
      currencyCode: "USD",
      winningBidId: null,
      winnerId: null,
      finalAmountMinor: null,
    };

    const decision = decideRelay({
      key: "42",
      value: JSON.stringify(closed),
      eventType: "AuctionClosed.v1",
    });

    expect(decision).toMatchObject({ kind: "publish", channel: "auction:42" });
    if (decision.kind !== "publish") {
      throw new Error("expected a publish");
    }
    expect(JSON.parse(decision.message)).toEqual({
      v: 1,
      eventType: "AuctionClosed.v1",
      event: closed,
    });
  });

  it("skips an event type without a relay contract", () => {
    expect(decideRelay({ ...record(), eventType: "AuctionRenamed.v9" })).toEqual({
      kind: "skip",
      reason: "unknown-event-type",
      detail: "no relay contract for AuctionRenamed.v9",
    });
  });

  it.each([
    ["no event-type header", { eventType: undefined }, "missing event-type header"],
    ["no value", { value: null }, "missing value"],
    ["invalid JSON", { value: "{" }, "JSON"],
    [
      "a payload that breaks the contract",
      { value: JSON.stringify({ ...JSON.parse(record().value!), amountMinor: "-5" }) },
      "BidAccepted.v1 contract",
    ],
    [
      "a header that disagrees with the payload's event type",
      { value: JSON.stringify({ ...JSON.parse(record().value!), eventType: "Other.v1" }) },
      "BidAccepted.v1 contract",
    ],
    ["a key for another auction", { key: "43" }, "key 43 does not match auctionId 42"],
    ["no key", { key: null }, "key null does not match auctionId 42"],
  ])("skips a record with %s as malformed", (_case, override, detail) => {
    const decision = decideRelay({ ...record("42"), ...override });

    expect(decision).toMatchObject({ kind: "skip", reason: "malformed" });
    if (decision.kind !== "skip") {
      throw new Error("expected a skip");
    }
    expect(decision.detail).toContain(detail);
  });
});
