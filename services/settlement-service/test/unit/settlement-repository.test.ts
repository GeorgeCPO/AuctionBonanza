import { beforeEach, describe, expect, it } from "vitest";

import { PermanentSettlementError } from "../../src/settlement.errors.js";
import { PostgresSettlementRepository } from "../../src/settlement.repository.js";
import { soldEvent, unsoldEvent } from "../support/events.js";
import { FakeDatabase } from "../support/fake-database.js";

// The shape of the transaction, against a scripted database. What the SQL
// does to real rows is proven by the integration tests.
describe("PostgresSettlementRepository", () => {
  let database: FakeDatabase;
  let repository: PostgresSettlementRepository;

  beforeEach(() => {
    database = new FakeDatabase();
    repository = new PostgresSettlementRepository(database);
  });

  // The first delivery of an event: recording it returns its id, the order
  // insert returns the new order, and the auction is recorded as closed unsold.
  function firstDelivery(orderId = "501"): void {
    database.respond = (text) => {
      if (text.includes("INSERT INTO settlement_processed_events")) {
        return [{ event_id: "recorded" }];
      }
      if (text.includes("INSERT INTO orders")) {
        return [{ id: orderId }];
      }
      if (text.includes("FROM auctions")) {
        return [{ unsold: true }];
      }
      return undefined;
    };
  }

  it("records the event and creates the order in one transaction", async () => {
    firstDelivery("501");
    const event = soldEvent();

    const outcome = await repository.settle(event);

    expect(outcome).toEqual({ kind: "order_created", orderId: "501" });
    expect(database.statements()).toEqual([
      "BEGIN",
      expect.stringContaining("INSERT INTO settlement_processed_events"),
      expect.stringContaining("INSERT INTO orders"),
      "COMMIT",
    ]);
    expect(database.queries[1]?.values).toEqual([
      event.eventId,
      "AuctionClosed.v1",
      event.auctionId,
      event.auctionVersion,
    ]);
    expect(database.queries[2]?.values).toEqual([
      event.auctionId,
      event.winningBidId,
      event.winnerId,
      event.finalAmountMinor,
      event.currencyCode,
      event.eventId,
    ]);
    expect(database.releases).toEqual([undefined]);
  });

  it("records an unsold auction without creating an order, after checking it is unsold", async () => {
    firstDelivery();
    const event = unsoldEvent();

    const outcome = await repository.settle(event);

    expect(outcome).toEqual({ kind: "unsold_recorded" });
    expect(database.statements()).toEqual([
      "BEGIN",
      expect.stringContaining("INSERT INTO settlement_processed_events"),
      expect.stringContaining("FROM auctions"),
      "COMMIT",
    ]);
    expect(database.queries[2]?.values).toEqual([event.auctionId]);
  });

  describe("an event that says unsold", () => {
    it.each([
      ["an auction that has a winner", [{ unsold: false }]],
      ["an auction that is not closed", [{ unsold: false }]],
      ["an auction that does not exist", []],
    ])("is refused, and not recorded, for %s", async (_description, auctionRows) => {
      database.respond = (text) => {
        if (text.includes("INSERT INTO settlement_processed_events")) {
          return [{ event_id: "recorded" }];
        }
        return text.includes("FROM auctions") ? auctionRows : undefined;
      };

      const failure = await repository.settle(unsoldEvent()).catch((error: unknown) => error);

      expect(failure).toBeInstanceOf(PermanentSettlementError);
      expect(failure).toMatchObject({
        sqlState: undefined,
        constraint: "settlement_unsold_matches_auction",
      });
      expect(database.statements().at(-1)).toBe("ROLLBACK");
      expect(database.statements()).not.toContain("COMMIT");
    });
  });

  it("recognises an event it handled before and writes nothing else", async () => {
    // ON CONFLICT DO NOTHING returns no row when the event is already there.
    database.respond = () => [];

    const outcome = await repository.settle(soldEvent());

    expect(outcome).toEqual({ kind: "duplicate" });
    expect(database.statements()).toEqual([
      "BEGIN",
      expect.stringContaining("INSERT INTO settlement_processed_events"),
      "COMMIT",
    ]);
    expect(database.releases).toEqual([undefined]);
  });

  describe("when a statement fails", () => {
    function failOrderInsert(error: Error): void {
      database.respond = (text) => {
        if (text.includes("INSERT INTO settlement_processed_events")) {
          return [{ event_id: "recorded" }];
        }
        if (text.includes("INSERT INTO orders")) {
          throw error;
        }
        return undefined;
      };
    }

    it("rolls back, destroys the connection, and reports a data error as permanent", async () => {
      const refused = Object.assign(new Error("duplicate key value"), {
        code: "23505",
        constraint: "orders_auction_id_key",
      });
      failOrderInsert(refused);

      const failure = await repository.settle(soldEvent()).catch((error: unknown) => error);

      expect(failure).toBeInstanceOf(PermanentSettlementError);
      expect(failure).toMatchObject({ sqlState: "23505", constraint: "orders_auction_id_key" });
      expect(database.statements().at(-1)).toBe("ROLLBACK");
      expect(database.statements()).not.toContain("COMMIT");
      expect(database.releases).toEqual([refused]);
    });

    it("rolls back and rethrows any other error unchanged, so it is retried", async () => {
      const outage = Object.assign(new Error("terminating connection"), { code: "57P01" });
      failOrderInsert(outage);

      await expect(repository.settle(soldEvent())).rejects.toBe(outage);

      expect(database.statements().at(-1)).toBe("ROLLBACK");
      expect(database.releases).toEqual([outage]);
    });

    it("reports the failed COMMIT, not the rollback that follows it", async () => {
      const lost = new Error("connection lost during commit");
      firstDelivery();
      const respond = database.respond;
      database.respond = (text) => {
        if (text === "COMMIT") {
          throw lost;
        }
        return respond(text);
      };

      await expect(repository.settle(soldEvent())).rejects.toBe(lost);

      expect(database.releases).toEqual([lost]);
    });

    it("still reports the original error when the rollback fails too", async () => {
      const outage = new Error("the original failure");
      failOrderInsert(outage);
      const respond = database.respond;
      database.respond = (text) => {
        if (text === "ROLLBACK") {
          throw new Error("connection already gone");
        }
        return respond(text);
      };

      await expect(repository.settle(soldEvent())).rejects.toBe(outage);

      expect(database.releases).toEqual([outage]);
    });

    it("fails if the order insert returns no row", async () => {
      database.respond = (text) =>
        text.includes("INSERT INTO settlement_processed_events") ? [{ event_id: "x" }] : [];

      await expect(repository.settle(soldEvent({ auctionId: "42" }))).rejects.toThrow(
        "inserting the order for auction 42 returned no row",
      );

      expect(database.statements()).not.toContain("COMMIT");
    });
  });
});
