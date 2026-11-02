import { describe, expect, it, vi } from "vitest";

import { AuctionApi } from "../../src/api/auction.api.js";
import { HttpClient } from "../../src/api/http.js";
import { IdentityApi } from "../../src/api/identity.api.js";

// Each call must reach the right path with the right verb, body and token:
// the UI has no other check on them.
function fake(status = 200, body: unknown = {}) {
  const fetchFunction = vi.fn<typeof fetch>(() =>
    Promise.resolve(new Response(status === 204 ? null : JSON.stringify(body), { status })),
  );
  return fetchFunction;
}

function lastCall(fetchFunction: ReturnType<typeof fake>): { url: string; init: RequestInit } {
  const call = fetchFunction.mock.lastCall;
  const input = call?.[0];
  return { url: typeof input === "string" ? input : "", init: call?.[1] ?? {} };
}

describe("IdentityApi", () => {
  it("registers with email and password", async () => {
    const fetchFunction = fake(201);
    const api = new IdentityApi(new HttpClient("/identity", fetchFunction));

    await api.register("a@example.com", "a-long-password");

    expect(lastCall(fetchFunction).url).toBe("/identity/v1/users");
    expect(JSON.parse(lastCall(fetchFunction).init.body as string)).toEqual({
      email: "a@example.com",
      password: "a-long-password",
    });
  });

  it("logs in, refreshes and logs out", async () => {
    const fetchFunction = fake(201);
    const api = new IdentityApi(new HttpClient("/identity", fetchFunction));

    await api.login("a@example.com", "pw");
    expect(lastCall(fetchFunction).url).toBe("/identity/v1/sessions");

    await api.refresh("r1");
    expect(lastCall(fetchFunction).url).toBe("/identity/v1/sessions/refresh");
    expect(lastCall(fetchFunction).init.body).toBe('{"refreshToken":"r1"}');

    const logoutFetch = fake(204);
    await new IdentityApi(new HttpClient("/identity", logoutFetch)).logout("tok", "r1");
    expect(lastCall(logoutFetch).init.method).toBe("DELETE");
    expect(lastCall(logoutFetch).init.headers).toMatchObject({ authorization: "Bearer tok" });
  });
});

describe("AuctionApi", () => {
  it("reads auctions without a token", async () => {
    const fetchFunction = fake();
    const api = new AuctionApi(new HttpClient("/auction", fetchFunction));

    await api.list(5);
    expect(lastCall(fetchFunction).url).toBe("/auction/v1/auctions?limit=5");

    await api.get("42");
    expect(lastCall(fetchFunction).url).toBe("/auction/v1/auctions/42");

    expect(lastCall(fetchFunction).init.headers).not.toHaveProperty("authorization");
  });

  it("creates auctions and places bids with the token", async () => {
    const fetchFunction = fake(201);
    const api = new AuctionApi(new HttpClient("/auction", fetchFunction));

    await api.create("tok", {
      title: "t",
      description: "d",
      currencyCode: "USD",
      endsAt: "2030-01-01T00:00:00.000Z",
      startingPriceMinor: "1000",
      minimumBidIncrementMinor: "100",
    });
    expect(lastCall(fetchFunction).url).toBe("/auction/v1/auctions");
    expect(lastCall(fetchFunction).init.headers).toMatchObject({ authorization: "Bearer tok" });

    await api.placeBid("tok", "42", "1100", "key-1");
    expect(lastCall(fetchFunction).url).toBe("/auction/v1/auctions/42/bids");
    expect(lastCall(fetchFunction).init.body).toBe(
      '{"amountMinor":"1100","idempotencyKey":"key-1"}',
    );
  });
});
