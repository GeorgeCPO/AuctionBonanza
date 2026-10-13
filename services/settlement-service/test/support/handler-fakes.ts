import type { AuctionClosedV1 } from "@auction-bonanza/contracts";

import type { DeadLetterPublisher, DeadLetterRecord } from "../../src/dead-letter.types.js";
import type {
  SettlementOutcome,
  SettlementRepository,
} from "../../src/settlement.repository.types.js";

// Answers each settle() from a script and remembers what it was asked.
export class ScriptedRepository implements SettlementRepository {
  readonly settled: AuctionClosedV1[] = [];
  // What the next calls do. A function may throw, to script a failure.
  outcome: (event: AuctionClosedV1) => Promise<SettlementOutcome> | SettlementOutcome = () => ({
    kind: "order_created",
    orderId: "1",
  });

  async settle(event: AuctionClosedV1): Promise<SettlementOutcome> {
    this.settled.push(event);
    return this.outcome(event);
  }
}

// Remembers every dead letter it was given. `failWith` makes publishes reject.
export class RecordingDeadLetters implements DeadLetterPublisher {
  readonly published: DeadLetterRecord[] = [];
  failWith: Error | undefined;

  async publish(record: DeadLetterRecord): Promise<void> {
    if (this.failWith !== undefined) {
      throw this.failWith;
    }
    this.published.push(record);
  }
}
