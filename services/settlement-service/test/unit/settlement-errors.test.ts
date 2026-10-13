import { describe, expect, it } from "vitest";

import { classifySettlementError, PermanentSettlementError } from "../../src/settlement.errors.js";

// pg's DatabaseError carries the SQLSTATE in `code` and the violated
// constraint in `constraint`.
function databaseError(code: string, constraint?: string): Error {
  return Object.assign(new Error(`database error ${code}`), {
    code,
    ...(constraint === undefined ? {} : { constraint }),
  });
}

describe("classifySettlementError", () => {
  it.each([
    ["a unique violation", "23505"],
    ["a foreign key violation", "23503"],
    ["a check violation, as our triggers raise", "23514"],
    ["a numeric value out of range", "22003"],
    ["an invalid text representation", "22P02"],
  ])("treats %s as permanent", (_description, code) => {
    const original = databaseError(code, "orders_auction_id_key");

    const classified = classifySettlementError(original);

    expect(classified).toBeInstanceOf(PermanentSettlementError);
    expect(classified).toMatchObject({
      sqlState: code,
      constraint: "orders_auction_id_key",
      message: original.message,
      cause: original,
    });
  });

  it("leaves the constraint undefined when the error names none", () => {
    expect(classifySettlementError(databaseError("22003"))).toMatchObject({
      constraint: undefined,
    });
  });

  it.each([
    ["a serialization failure", "40001"],
    ["a deadlock", "40P01"],
    ["a lost connection", "08006"],
    ["a statement timeout", "57014"],
    ["a lock timeout", "55P03"],
    ["an insufficient resources error", "53200"],
    ["a code it does not know", "XX000"],
  ])("treats %s as transient, so it is retried", (_description, code) => {
    const original = databaseError(code);

    expect(classifySettlementError(original)).toBe(original);
  });

  it.each([
    ["a Node system error", Object.assign(new Error("connect failed"), { code: "ECONNREFUSED" })],
    ["a five-letter Node system error", Object.assign(new Error("odd"), { code: "EPIPE" })],
    ["a numeric code", Object.assign(new Error("odd"), { code: 23505 })],
    ["an error without a code", new Error("plain")],
  ])("treats %s as transient", (_description, error) => {
    expect(classifySettlementError(error)).toBe(error);
  });

  it("returns a value that is not an error unchanged", () => {
    expect(classifySettlementError("boom")).toBe("boom");
  });
});
