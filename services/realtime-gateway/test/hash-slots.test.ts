import { describe, expect, it } from "vitest";

import { auctionChannel } from "../src/bus/bus.js";
import { auctionIdsInSlot, slotOf } from "./support/hash-slots.js";

describe("slotOf", () => {
  // Values from the Redis cluster specification and the CLUSTER KEYSLOT docs.
  it.each([
    ["123456789", 12739],
    ["somekey", 11058],
    ["auction:1234567890", 10997],
  ])("hashes %j to slot %i", (name, slot) => {
    expect(slotOf(name)).toBe(slot);
  });
});

describe("auctionIdsInSlot", () => {
  it("finds distinct ids whose channels are in the slot", () => {
    const ids = auctionIdsInSlot(2947, 3, 4_000_000_000);

    expect(new Set(ids).size).toBe(3);
    expect(ids.map((id) => slotOf(auctionChannel(id)))).toEqual([2947, 2947, 2947]);
  });
});
