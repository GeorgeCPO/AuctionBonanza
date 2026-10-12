import type { AuthenticatedPrincipal } from "@auction-bonanza/access-auth";
import Fastify from "fastify";
import { afterEach, describe, expect, it, vi } from "vitest";

import { createAuthenticationHook } from "../../src/authentication.js";

const apps: ReturnType<typeof Fastify>[] = [];

afterEach(async () => {
  await Promise.all(apps.splice(0).map(async (app) => app.close()));
});

function makeApp(verify: (token: string) => Promise<AuthenticatedPrincipal>) {
  const app = Fastify();
  apps.push(app);
  app.decorateRequest("auctionPrincipal", null);
  // oxlint-disable-next-line typescript/no-misused-promises -- Fastify explicitly supports async pre-handler hooks.
  app.post("/auctions", { preHandler: createAuthenticationHook({ verify }) }, async (request) => ({
    principal: request.auctionPrincipal,
  }));
  return app;
}

describe("auction authentication hook", () => {
  it("parses a bearer token and attaches its verified principal", async () => {
    const verify = vi.fn<(token: string) => Promise<AuthenticatedPrincipal>>(async () => ({
      userId: "42",
      sessionId: "session-1",
      tokenId: "token-1",
    }));
    const app = makeApp(verify);

    const response = await app.inject({
      method: "POST",
      url: "/auctions",
      headers: { authorization: "Bearer signed-token" },
    });

    expect(response.statusCode).toBe(200);
    expect(verify).toHaveBeenCalledWith("signed-token");
    expect(response.json().principal.userId).toBe("42");
  });

  it.each([
    [undefined, "42"],
    ["Bearer rejected", "42"],
    ["Bearer signed-token", "not-a-bigint-id"],
  ])("fails closed for invalid credentials", async (authorization, userId) => {
    const verify = vi.fn<(token: string) => Promise<AuthenticatedPrincipal>>(async () => {
      if (authorization === "Bearer rejected") {
        throw new Error("invalid signature");
      }
      return { userId, sessionId: "session-1", tokenId: "token-1" };
    });
    const app = makeApp(verify);

    const response = await app.inject({
      method: "POST",
      url: "/auctions",
      ...(authorization === undefined ? {} : { headers: { authorization } }),
    });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({
      code: "UNAUTHORIZED",
      message: "Authentication is required",
    });
  });
});
