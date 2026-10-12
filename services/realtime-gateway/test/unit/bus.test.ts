import { describe, expect, it } from "vitest";

import { auctionChannel, auctionIdOfChannel } from "../../src/bus/bus.js";

describe("auctionIdOfChannel", () => {
  it("is the inverse of auctionChannel", () => {
    expect(auctionIdOfChannel(auctionChannel("9223372036854775807"))).toBe("9223372036854775807");
  });

  it.each(["", "auction", "other:42", "AUCTION:42"])(
    "returns undefined for %j, which is not an auction's channel",
    (channel) => {
      expect(auctionIdOfChannel(channel)).toBeUndefined();
    },
  );
});
