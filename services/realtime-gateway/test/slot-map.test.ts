import { describe, expect, it } from "vitest";

import { slotOf } from "../src/bus/hash-slots.js";
import { SlotMap } from "../src/bus/slot-map.js";

describe("SlotMap.fromShards", () => {
  const shards = [
    {
      slots: [0, 5460],
      nodes: [
        { id: "r1", role: "replica", endpoint: "localhost", port: 7004 },
        { id: "a", role: "master", endpoint: "localhost", port: 7001 },
      ],
    },
    {
      slots: [5461, 5470, 5472, 10922],
      nodes: [{ id: "b", role: "master", endpoint: "", ip: "10.0.0.2", port: 7002 }],
    },
    {
      slots: [10923, 16383],
      nodes: [{ id: "c", role: "master", endpoint: "localhost", port: 7003 }],
    },
  ];

  it("maps each slot to the primary owning it", () => {
    const map = SlotMap.fromShards(shards);

    expect(map.ownerOf(0)).toBe("localhost:7001");
    expect(map.ownerOf(5460)).toBe("localhost:7001");
    expect(map.ownerOf(5461)).toBe("10.0.0.2:7002");
    expect(map.ownerOf(10922)).toBe("10.0.0.2:7002");
    expect(map.ownerOf(16383)).toBe("localhost:7003");
  });

  it("returns undefined for a slot no primary owns", () => {
    expect(SlotMap.fromShards(shards).ownerOf(5471)).toBeUndefined();
  });

  it("lists the primaries, not the replicas", () => {
    expect([...SlotMap.fromShards(shards).primaryAddresses()].toSorted()).toEqual([
      "10.0.0.2:7002",
      "localhost:7001",
      "localhost:7003",
    ]);
  });
});

describe("slotOf with hash tags", () => {
  it("hashes only the first non-empty {tag}", () => {
    expect(slotOf("a{user1}b")).toBe(slotOf("user1"));
    expect(slotOf("a{}b")).toBe(slotOf("a{}b"));
    expect(slotOf("{}{x}")).toBe(slotOf("{}{x}"));
    expect(slotOf("a{b{c}d")).toBe(slotOf("b{c"));
    expect(slotOf("a{b")).toBe(slotOf("a{b"));
  });
});
