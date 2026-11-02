import { describe, expect, it, vi } from "vitest";

import { ApiError, HttpClient } from "../../src/api/http.js";
import { describeError } from "../../src/error-messages.js";

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

describe("HttpClient", () => {
  it("sends JSON and the bearer token under the service prefix", async () => {
    const fetchFunction = vi.fn<() => Promise<Response>>(() =>
      Promise.resolve(jsonResponse(201, { ok: true })),
    );
    const client = new HttpClient("/auction", fetchFunction);

    const result = await client.request("/v1/auctions", {
      method: "POST",
      body: { a: 1 },
      accessToken: "tok",
    });

    expect(result).toEqual({ ok: true });
    expect(fetchFunction).toHaveBeenCalledWith("/auction/v1/auctions", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer tok" },
      body: '{"a":1}',
    });
  });

  it("returns undefined for 204", async () => {
    const client = new HttpClient("/identity", () =>
      Promise.resolve(new Response(null, { status: 204 })),
    );

    expect(await client.request("/v1/sessions/current", { method: "DELETE" })).toBeUndefined();
  });

  it("throws ApiError carrying the code and extra fields", async () => {
    const client = new HttpClient("/auction", () =>
      Promise.resolve(
        jsonResponse(422, {
          code: "BID_TOO_LOW",
          message: "low",
          minimumQualifyingAmountMinor: "1100",
        }),
      ),
    );

    const error = await client.request("/x").catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({ status: 422, code: "BID_TOO_LOW" });
    expect(describeError(error)).toBe("Bid too low: the minimum is 11.00.");
  });

  it("reports a non-JSON error response as an unexpected response", async () => {
    const client = new HttpClient("/auction", () =>
      Promise.resolve(new Response("<html>bad gateway</html>", { status: 502 })),
    );

    const error = await client.request("/x").catch((caught: unknown) => caught);

    expect(error).toMatchObject({ status: 502, code: "UNEXPECTED_RESPONSE" });
  });
});

describe("describeError", () => {
  function apiError(code: string, extra: Record<string, unknown> = {}): ApiError {
    return new ApiError({ status: 400, body: { code, message: "m", ...extra } });
  }

  it.each([
    [apiError("AUCTION_NOT_OPEN", { phase: "closed" }), "The auction is not open (closed)."],
    [apiError("SELF_BID_NOT_ALLOWED"), "You cannot bid on your own auction."],
    [apiError("UNAUTHORIZED"), "Not signed in, or the session expired. Sign in again."],
    [apiError("EMAIL_ALREADY_REGISTERED"), "That email is already registered. Use Sign in."],
    [apiError("BID_TOO_LOW"), "Bid too low."],
    [apiError("SOMETHING_ELSE"), "SOMETHING_ELSE: m"],
    [new Error("network down"), "network down"],
    ["odd", "Something went wrong"],
  ])("describes %#", (error, expected) => {
    expect(describeError(error)).toBe(expected);
  });
});
