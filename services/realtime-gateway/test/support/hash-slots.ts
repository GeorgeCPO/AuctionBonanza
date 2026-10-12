import { auctionChannel } from "../../src/bus/bus.js";
import { slotOf } from "../../src/bus/hash-slots.js";

export { slotOf };

// The first `count` auction ids at or after `firstCandidate` whose channels
// are in the slot. About one id in 16384 is, so this scans some tens of
// thousands of ids per auction: cheap.
export function auctionIdsInSlot(slot: number, count: number, firstCandidate: number): string[] {
  const ids: string[] = [];
  for (let candidate = firstCandidate; ids.length < count; candidate += 1) {
    const id = String(candidate);
    if (slotOf(auctionChannel(id)) === slot) {
      ids.push(id);
    }
  }
  return ids;
}
