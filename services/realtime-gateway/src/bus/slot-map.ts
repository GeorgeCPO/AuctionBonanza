import type { ClusterShardReply, SlotRange } from "./slot-map.types.js";

// Which primary owns each hash slot, as of one CLUSTER SHARDS reply. It never
// changes after it is built; a newer reply makes a new SlotMap.
export class SlotMap {
  private readonly ranges: readonly SlotRange[];

  constructor(ranges: readonly SlotRange[]) {
    this.ranges = [...ranges].sort((left, right) => left.start - right.start);
  }

  static fromShards(shards: readonly ClusterShardReply[]): SlotMap {
    const ranges: SlotRange[] = [];
    for (const shard of shards) {
      const primary = shard.nodes.find((node) => node.role === "master");
      const host = primary?.endpoint || primary?.ip;
      if (primary === undefined || !host || primary.port === undefined) {
        continue;
      }
      for (let index = 0; index + 1 < shard.slots.length; index += 2) {
        ranges.push({
          start: shard.slots[index]!,
          end: shard.slots[index + 1]!,
          address: `${host}:${primary.port}`,
        });
      }
    }
    return new SlotMap(ranges);
  }

  // The primary owning the slot, or undefined when no primary does.
  ownerOf(slot: number): string | undefined {
    // The ranges are sorted and do not overlap.
    let low = 0;
    let high = this.ranges.length - 1;
    while (low <= high) {
      const middle = (low + high) >> 1;
      const range = this.ranges[middle]!;
      if (slot < range.start) {
        high = middle - 1;
      } else if (slot > range.end) {
        low = middle + 1;
      } else {
        return range.address;
      }
    }
    return undefined;
  }

  primaryAddresses(): ReadonlySet<string> {
    return new Set(this.ranges.map((range) => range.address));
  }
}
