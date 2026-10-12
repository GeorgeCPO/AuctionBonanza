import { pino } from "pino";
import { describe, expect, it } from "vitest";

import { captureLogger } from "./support/outbox-fixtures.js";
import { FinalizationWorker } from "../src/auctions/finalization.worker.js";
import type {
  FinalizationOutcome,
  FinalizationRepository,
} from "../src/auctions/finalization.types.js";

// Plays back a script of outcomes, or throws the scripted error, one per call.
class ScriptedRepository implements FinalizationRepository {
  calls = 0;

  constructor(private readonly script: (FinalizationOutcome | Error)[]) {}

  async finalizeNextDue(): Promise<FinalizationOutcome> {
    const step = this.script[this.calls] ?? { kind: "none_due" };
    this.calls += 1;
    if (step instanceof Error) {
      throw step;
    }
    return step;
  }
}

function worker(repository: FinalizationRepository): FinalizationWorker {
  return new FinalizationWorker({
    repository,
    pollIntervalMs: 5,
    retryBackoff: { baseDelayMs: 5, maxDelayMs: 5 },
    logger: pino({ level: "silent" }),
    random: () => 0,
  });
}

const sold: FinalizationOutcome = {
  kind: "closed",
  auctionId: "1",
  auctionVersion: "3",
  winningBidId: "9",
};
const unsold: FinalizationOutcome = {
  kind: "closed",
  auctionId: "2",
  auctionVersion: "2",
  winningBidId: null,
};

describe("FinalizationWorker", () => {
  it("drains until nothing is due and counts what it closed", async () => {
    const repository = new ScriptedRepository([sold, unsold, sold]);

    await expect(worker(repository).drain()).resolves.toBe(3);

    expect(repository.calls).toBe(4);
  });

  it("stops draining between closes once the signal aborts", async () => {
    const shutdown = new AbortController();
    const repository = new ScriptedRepository([sold, sold, sold]);
    const original = repository.finalizeNextDue.bind(repository);
    repository.finalizeNextDue = async () => {
      const outcome = await original();
      shutdown.abort();
      return outcome;
    };

    await expect(worker(repository).drain(shutdown.signal)).resolves.toBe(1);

    expect(repository.calls).toBe(1);
  });

  it("logs a failed close, keeps draining, and does not count it as closed", async () => {
    const { logger, entries } = captureLogger();
    const repository = new ScriptedRepository([
      {
        kind: "close_failed",
        auctionId: "7",
        attempt: 2,
        retryDelayMs: 4_000,
        error: new Error("poisoned"),
      },
      sold,
    ]);

    const closed = await new FinalizationWorker({
      repository,
      pollIntervalMs: 5,
      retryBackoff: { baseDelayMs: 5, maxDelayMs: 5 },
      logger,
    }).drain();

    expect(closed).toBe(1);
    expect(repository.calls).toBe(3);
    expect(entries).toContainEqual(
      expect.objectContaining({
        level: 50,
        auctionId: "7",
        attempt: 2,
        retryDelayMs: 4_000,
        msg: "auction close failed; it will be retried and does not block other auctions",
      }),
    );
    expect(entries.some((entry) => entry.msg === "auction finalization failed")).toBe(false);
  });

  it("propagates a failure from drain so the loop can back off", async () => {
    const repository = new ScriptedRepository([sold, new Error("database unavailable")]);

    await expect(worker(repository).drain()).rejects.toThrow("database unavailable");
  });

  it("keeps running after a failed cycle and resumes closing auctions", async () => {
    const repository = new ScriptedRepository([new Error("database unavailable"), sold]);
    const shutdown = new AbortController();

    const running = worker(repository).run(shutdown.signal);
    while (repository.calls < 3) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    shutdown.abort();
    await running;

    // failure, then the sold close, then the empty poll that ends the drain
    expect(repository.calls).toBeGreaterThanOrEqual(3);
  });

  it("returns promptly when the signal is already aborted", async () => {
    const repository = new ScriptedRepository([]);
    const shutdown = new AbortController();
    shutdown.abort();

    await worker(repository).run(shutdown.signal);

    expect(repository.calls).toBe(0);
  });
});
