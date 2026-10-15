import { describe, expect, it } from "vitest";

import { addMinor, formatMinor, formatMoney, parseMajorToMinor } from "../../src/money.js";

describe("formatMinor", () => {
  it.each([
    ["0", "0.00"],
    ["5", "0.05"],
    ["100", "1.00"],
    ["1050", "10.50"],
    ["12345678901234567890", "123456789012345678.90"],
  ])("formats %s as %s", (input, expected) => {
    expect(formatMinor(input)).toBe(expected);
  });

  it("leaves a value that is not digits unchanged", () => {
    expect(formatMinor("12.5")).toBe("12.5");
  });
});

describe("formatMoney", () => {
  it("appends the currency", () => {
    expect(formatMoney("1050", "EUR")).toBe("10.50 EUR");
  });
});

describe("parseMajorToMinor", () => {
  it.each([
    ["10", "1000"],
    ["10.5", "1050"],
    ["10.05", "1005"],
    ["0.99", "99"],
    ["007.10", "710"],
    [" 3.00 ", "300"],
  ])("parses %s as %s", (input, expected) => {
    expect(parseMajorToMinor(input)).toBe(expected);
  });

  it.each(["", "abc", "-1", "1.234", "1,50", "0", "0.00", "1.", ".5"])("rejects %j", (input) => {
    expect(parseMajorToMinor(input)).toBeUndefined();
  });
});

describe("addMinor", () => {
  it("adds without losing precision past 2^53", () => {
    expect(addMinor("9007199254740993", "10")).toBe("9007199254741003");
  });
});
