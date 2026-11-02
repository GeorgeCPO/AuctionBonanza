import { describe, expect, it } from "vitest";

import { formatRoute, parseRoute } from "../../src/ui/route.js";

describe("parseRoute", () => {
  it.each([
    ["", { name: "list" }],
    ["#/", { name: "list" }],
    ["#/new", { name: "new" }],
    ["#/auctions/42", { name: "auction", auctionId: "42" }],
    ["#/auctions/abc", { name: "list" }],
    ["#/nope", { name: "list" }],
  ])("parses %j", (hash, expected) => {
    expect(parseRoute(hash)).toEqual(expected);
  });
});

describe("formatRoute", () => {
  it.each([{ name: "list" }, { name: "new" }, { name: "auction", auctionId: "42" }] as const)(
    "round-trips %j through parseRoute",
    (route) => {
      expect(parseRoute(formatRoute(route))).toEqual(route);
    },
  );
});
