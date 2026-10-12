import type { AuthenticatedPrincipal } from "@auction-bonanza/access-auth";
import type { preHandlerAsyncHookHandler } from "fastify";
import { afterEach, describe, expect, it, vi } from "vitest";

import { buildApp } from "../src/app.js";
import type { Auction, AuctionLiveState } from "../src/auctions/auction.js";
import { AuctionService, type AuctionServicePort } from "../src/auctions/auction.service.js";
import type { AuctionRepository } from "../src/auctions/auction.repository.js";
import type { Bid } from "../src/bids/bid.js";
import type { BidServicePort } from "../src/bids/bid.service.js";
import type { AuctionConfig } from "../src/config.js";
import type { Database } from "../src/database.js";

const apps: ReturnType<typeof buildApp>[] = [];
const config: AuctionConfig = {
  nodeEnv: "test",
  host: "127.0.0.1",
  port: 3002,
  databaseUrl: "postgresql://test:test@localhost:5432/auction_db",
  authIssuer: "https://identity.test",
  authAudience: "auction-api",
  accessTokenTtlSeconds: 600,
  accessTokenClockSkewSeconds: 5,
  authJwksUrl: "https://identity.test/.well-known/jwks.json",
  authJwksCacheTtlMilliseconds: 300_000,
  supportedCurrencyCodes: ["USD"],
};
const principal: AuthenticatedPrincipal = {
  userId: "42",
  sessionId: "session-1",
  tokenId: "token-1",
};
const auction: Auction = {
  id: "91",
  ownerId: "42",
  title: "Vintage clock",
  description: "A mechanical clock.",
  currencyCode: "USD",
  startsAt: new Date("2030-01-01T10:00:00.000Z"),
  endsAt: new Date("2030-01-01T11:00:00.000Z"),
  startingPriceMinor: "1000",
  minimumBidIncrementMinor: "100",
  version: "1",
  cancelledAt: null,
  closedAt: null,
  winningBidId: null,
  createdAt: new Date("2029-12-01T10:00:00.000Z"),
  updatedAt: new Date("2029-12-01T10:00:00.000Z"),
  phase: "scheduled",
};
const liveState: AuctionLiveState = {
  auctionId: "91",
  auctionVersion: "3",
  currencyCode: "USD",
  startsAt: new Date("2030-01-01T10:00:00.000Z"),
  endsAt: new Date("2030-01-01T11:00:00.000Z"),
  cancelledAt: null,
  closedAt: null,
  phase: "open",
  leadingBid: { bidId: "501", amountMinor: "1200" },
};
const validPayload = {
  title: "Vintage clock",
  description: "A mechanical clock.",
  currencyCode: "USD",
  startsAt: "2030-01-01T10:00:00.000Z",
  endsAt: "2030-01-01T11:00:00.000Z",
  startingPriceMinor: "1000",
  minimumBidIncrementMinor: "100",
};
const bid: Bid = {
  id: "500",
  auctionId: "91",
  bidderId: "42",
  amountMinor: "1100",
  idempotencyKey: "client-key-1",
  createdAt: new Date("2030-01-01T10:05:00.000Z"),
};
const bidAuctionState = {
  id: "91",
  version: "2",
  leadingBidId: "500",
  updatedAt: new Date("2030-01-01T10:05:00.000Z"),
};
const validBidPayload = { amountMinor: "1100", idempotencyKey: "client-key-1" };

afterEach(async () => {
  await Promise.all(apps.splice(0).map(async (app) => app.close()));
});

function makeDatabase(): Database {
  return {
    query: vi.fn<Database["query"]>(async () => ({ rows: [] })),
    connect: vi.fn<Database["connect"]>(async () =>
      Promise.reject(new Error("unexpected transaction")),
    ),
  };
}

function makeAuctions(overrides: Partial<AuctionServicePort> = {}): AuctionServicePort {
  return {
    create: vi.fn<AuctionServicePort["create"]>(async () => ({ kind: "created", auction })),
    findById: vi.fn<AuctionServicePort["findById"]>(async () => auction),
    list: vi.fn<AuctionServicePort["list"]>(async () => ({
      kind: "found",
      page: { items: [auction], nextCursor: null },
    })),
    findLiveStates: vi.fn<AuctionServicePort["findLiveStates"]>(async () => ({
      kind: "found",
      liveStates: { items: [], missingIds: [], observedAt: new Date() },
    })),
    ...overrides,
  };
}

function makeBids(overrides: Partial<BidServicePort> = {}): BidServicePort {
  return {
    create: vi.fn<BidServicePort["create"]>(async () => ({
      kind: "placed",
      bid,
      auction: bidAuctionState,
      replay: false,
    })),
    ...overrides,
  };
}

const authenticate: preHandlerAsyncHookHandler = async (request) => {
  request.auctionPrincipal = principal;
};

function makeApp(
  auctions: AuctionServicePort,
  authentication = authenticate,
  bids: BidServicePort = makeBids(),
) {
  const app = buildApp({
    config,
    database: makeDatabase(),
    auctions,
    bids,
    authenticate: authentication,
  });
  apps.push(app);
  return app;
}

describe("auction HTTP API", () => {
  function makeListingApp() {
    const list = vi.fn<AuctionRepository["list"]>(async () => []);
    const repository: AuctionRepository = {
      create: vi.fn<AuctionRepository["create"]>(),
      findById: vi.fn<AuctionRepository["findById"]>(),
      list,
      findLiveStates: vi.fn<AuctionRepository["findLiveStates"]>(),
    };
    const deny = vi.fn<preHandlerAsyncHookHandler>(async (_request, reply) =>
      reply.code(401).send({ code: "UNAUTHORIZED" }),
    );
    return { app: makeApp(new AuctionService(repository, ["USD"]), deny), list, deny };
  }

  it.each([undefined, "", "   ", " ClOcK ", "%_\\"])(
    "normalizes public search %j and defaults the page size",
    async (search) => {
      const { app, list, deny } = makeListingApp();
      const query = new URLSearchParams();
      if (search !== undefined) {
        query.set("search", search);
      }
      const response = await app.inject(`/v1/auctions?${query.toString()}`);
      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ items: [], nextCursor: null });
      expect(list).toHaveBeenCalledWith({
        search: search?.trim() || undefined,
        limit: 21,
        cursor: undefined,
      });
      expect(deny).not.toHaveBeenCalled();
    },
  );

  it.each(["0", "101", "-1", "1.5", "1e2", "01", "", "NaN", " 2", "999999999999999999"])(
    "rejects invalid limit %j before querying PostgreSQL",
    async (limit) => {
      const { app, list } = makeListingApp();
      const response = await app.inject(
        `/v1/auctions?${new URLSearchParams({ limit }).toString()}`,
      );
      expect(response.statusCode).toBe(400);
      expect(list).not.toHaveBeenCalled();
    },
  );

  it.each([
    "",
    "!",
    "not-json",
    "e30=",
    ...[
      null,
      [],
      {},
      { createdAt: "2030-02-30T00:00:00.123456Z", id: "1" },
      { createdAt: "2030-01-01T24:00:00.123456Z", id: "1" },
      { createdAt: "0000-01-01T00:00:00.123456Z", id: "1" },
      { createdAt: "2030-01-01T00:00:00.123Z", id: "1" },
      { createdAt: "2030-01-01T00:00:00.123456Z", id: 1 },
      { createdAt: "2030-01-01T00:00:00.123456Z", id: "0" },
      { createdAt: "2030-01-01T00:00:00.123456Z", id: "9223372036854775808" },
      { createdAt: "2030-01-01T00:00:00.123456Z", id: "1", extra: true },
    ].map((value) => Buffer.from(JSON.stringify(value)).toString("base64url")),
  ])("rejects malformed cursor %j", async (cursor) => {
    const { app, list } = makeListingApp();
    const response = await app.inject(`/v1/auctions?${new URLSearchParams({ cursor }).toString()}`);
    expect(response.statusCode).toBe(400);
    expect(list).not.toHaveBeenCalled();
  });

  it.each(["limit=2&limit=3", "cursor=a&cursor=b", "search=a&search=b", "phase=open"])(
    "rejects unsupported or repeated query parameters %s",
    async (query) => {
      const { app, list } = makeListingApp();
      const response = await app.inject(`/v1/auctions?${query}`);
      expect(response.statusCode).toBe(400);
      expect(list).not.toHaveBeenCalled();
    },
  );

  it("accepts the maximum limit and preserves exact cursor values", async () => {
    const { app, list } = makeListingApp();
    const position = { createdAt: "2030-01-01T00:00:00.123456Z", id: "9223372036854775807" };
    const cursor = Buffer.from(JSON.stringify(position)).toString("base64url");
    const response = await app.inject(`/v1/auctions?limit=100&cursor=${cursor}`);
    expect(response.statusCode).toBe(200);
    expect(list).toHaveBeenCalledWith({ search: undefined, limit: 101, cursor: position });
  });

  it("returns only the requested items and uses the last returned row for the next cursor", async () => {
    const { app, list } = makeListingApp();
    const entries = ["9007199254740995", "9007199254740993", "91"].map((id) => ({
      auction: { ...auction, id },
      cursor: { createdAt: "2029-12-01T10:00:00.000456Z", id },
    }));
    list.mockResolvedValueOnce(entries).mockResolvedValueOnce(entries.slice(2));
    const first = await app.inject("/v1/auctions?limit=2");
    const firstPage = first.json<{ items: { id: string }[]; nextCursor: string }>();
    expect(first.statusCode).toBe(200);
    expect(firstPage.items.map((item) => item.id)).toEqual([
      "9007199254740995",
      "9007199254740993",
    ]);
    expect(JSON.parse(Buffer.from(firstPage.nextCursor, "base64url").toString())).toEqual(
      entries[1]!.cursor,
    );
    const second = await app.inject(`/v1/auctions?limit=2&cursor=${firstPage.nextCursor}`);
    expect(second.statusCode).toBe(200);
    expect(second.json()).toMatchObject({ items: [{ id: "91" }], nextCursor: null });
    expect(list).toHaveBeenLastCalledWith({
      search: undefined,
      limit: 3,
      cursor: entries[1]!.cursor,
    });
  });

  it("creates an owner-authenticated auction and identifies its resource", async () => {
    const create = vi.fn<AuctionServicePort["create"]>(async () => ({ kind: "created", auction }));
    const auctions = makeAuctions({ create });
    const app = makeApp(auctions);

    const response = await app.inject({
      method: "POST",
      url: "/v1/auctions",
      headers: { authorization: "Bearer access-token" },
      payload: validPayload,
    });

    expect(response.statusCode).toBe(201);
    expect(response.headers.location).toBe("/v1/auctions/91");
    expect(response.json()).toMatchObject({
      id: "91",
      ownerId: "42",
      startingPriceMinor: "1000",
      phase: "scheduled",
    });
    expect(create).toHaveBeenCalledWith("42", validPayload);
  });

  it("rejects creation when authentication fails", async () => {
    const create = vi.fn<AuctionServicePort["create"]>(async () => ({ kind: "created", auction }));
    const auctions = makeAuctions({ create });
    const deny: preHandlerAsyncHookHandler = async (_request, reply) =>
      reply.code(401).send({ code: "UNAUTHORIZED", message: "Authentication is required" });
    const app = makeApp(auctions, deny);

    const response = await app.inject({
      method: "POST",
      url: "/v1/auctions",
      payload: validPayload,
    });

    expect(response.statusCode).toBe(401);
    expect(create).not.toHaveBeenCalled();
  });

  it("rejects client-selected ownership", async () => {
    const create = vi.fn<AuctionServicePort["create"]>(async () => ({ kind: "created", auction }));
    const app = makeApp(makeAuctions({ create }));
    const response = await app.inject({
      method: "POST",
      url: "/v1/auctions",
      payload: { ...validPayload, ownerId: "99" },
    });
    expect(response.statusCode).toBe(400);
    expect(create).not.toHaveBeenCalled();
  });

  it("returns auction details with the repository-derived phase without authentication", async () => {
    const findById = vi.fn<AuctionServicePort["findById"]>(async () => auction);
    const auctions = makeAuctions({ findById });
    const app = makeApp(auctions);

    const response = await app.inject({ method: "GET", url: "/v1/auctions/91" });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      ...auction,
      startsAt: "2030-01-01T10:00:00.000Z",
      endsAt: "2030-01-01T11:00:00.000Z",
      cancelledAt: null,
      closedAt: null,
      createdAt: "2029-12-01T10:00:00.000Z",
      updatedAt: "2029-12-01T10:00:00.000Z",
    });
    expect(findById).toHaveBeenCalledWith("91");
  });

  it("returns 404 for an unknown auction", async () => {
    const app = makeApp(
      makeAuctions({
        findById: vi.fn<AuctionServicePort["findById"]>(async () => undefined),
      }),
    );

    const response = await app.inject({ method: "GET", url: "/v1/auctions/91" });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({
      code: "AUCTION_NOT_FOUND",
      message: "Auction was not found",
    });
  });

  it("lists auctions publicly using the existing response shape", async () => {
    const list = vi.fn<AuctionServicePort["list"]>(async () => ({
      kind: "found",
      page: { items: [auction], nextCursor: "next-page" },
    }));
    const app = makeApp(makeAuctions({ list }));

    const response = await app.inject({
      method: "GET",
      url: "/v1/auctions?search=%20clock%20&limit=5&cursor=current-page",
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      items: [
        {
          ...auction,
          startsAt: "2030-01-01T10:00:00.000Z",
          endsAt: "2030-01-01T11:00:00.000Z",
          cancelledAt: null,
          closedAt: null,
          createdAt: "2029-12-01T10:00:00.000Z",
          updatedAt: "2029-12-01T10:00:00.000Z",
        },
      ],
      nextCursor: "next-page",
    });
    expect(list).toHaveBeenCalledWith({
      search: " clock ",
      limit: "5",
      cursor: "current-page",
    });
  });

  it("rejects invalid pagination input", async () => {
    const list = vi.fn<AuctionServicePort["list"]>(async () => ({
      kind: "invalid",
      message: "cursor is invalid",
    }));
    const app = makeApp(makeAuctions({ list }));

    const response = await app.inject({
      method: "GET",
      url: "/v1/auctions?cursor=not-a-valid-cursor",
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({
      code: "INVALID_REQUEST",
      message: "Request validation failed",
    });
  });
});

describe("auction live-state HTTP API", () => {
  function makeLiveStateApp(states: AuctionLiveState[] = []) {
    const findLiveStates = vi.fn<AuctionRepository["findLiveStates"]>(async () => ({
      states,
      observedAt: new Date("2030-01-01T10:30:00.000Z"),
    }));
    const findById = vi.fn<AuctionRepository["findById"]>();
    const repository: AuctionRepository = {
      create: vi.fn<AuctionRepository["create"]>(),
      findById,
      list: vi.fn<AuctionRepository["list"]>(),
      findLiveStates,
    };
    const deny = vi.fn<preHandlerAsyncHookHandler>(async (_request, reply) =>
      reply.code(401).send({ code: "UNAUTHORIZED" }),
    );
    const app = makeApp(new AuctionService(repository, ["USD"]), deny);
    return { app, findLiveStates, findById, deny };
  }

  it("returns live states publicly in request order and lists missing auctions", async () => {
    const other: AuctionLiveState = {
      ...liveState,
      auctionId: "7",
      auctionVersion: "1",
      leadingBid: null,
    };
    const { app, findLiveStates, findById, deny } = makeLiveStateApp([other, liveState]);

    const response = await app.inject({
      method: "GET",
      url: "/v1/auctions/live-states?ids=91,404,7,91",
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      items: [
        {
          ...liveState,
          startsAt: "2030-01-01T10:00:00.000Z",
          endsAt: "2030-01-01T11:00:00.000Z",
        },
        {
          ...other,
          startsAt: "2030-01-01T10:00:00.000Z",
          endsAt: "2030-01-01T11:00:00.000Z",
        },
      ],
      missingIds: ["404"],
      observedAt: "2030-01-01T10:30:00.000Z",
    });
    // Repeated IDs are read once; the static segment never reaches /:id.
    expect(findLiveStates).toHaveBeenCalledWith(["91", "404", "7"]);
    expect(findById).not.toHaveBeenCalled();
    expect(deny).not.toHaveBeenCalled();
  });

  it("accepts the maximum number of IDs", async () => {
    const { app, findLiveStates } = makeLiveStateApp();
    const ids = Array.from({ length: 100 }, (_, index) => `922337203685477${5707 - index}`);

    const response = await app.inject({
      method: "GET",
      url: `/v1/auctions/live-states?ids=${ids.join(",")}`,
    });

    expect(response.statusCode).toBe(200);
    expect(findLiveStates).toHaveBeenCalledWith(ids);
  });

  it.each([
    ["missing ids", ""],
    ["an empty list", "?ids="],
    ["an empty element", "?ids=1,,2"],
    ["a trailing separator", "?ids=1,"],
    ["a non-numeric ID", "?ids=1,abc"],
    ["a zero ID", "?ids=0"],
    ["an ID above the bigint range", "?ids=9223372036854775808"],
    ["too many IDs", `?ids=${Array.from({ length: 101 }, (_, index) => index + 1).join(",")}`],
  ])("rejects %s before reading", async (_case, query) => {
    const { app, findLiveStates } = makeLiveStateApp();

    const response = await app.inject({ method: "GET", url: `/v1/auctions/live-states${query}` });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ code: "INVALID_REQUEST" });
    expect(findLiveStates).not.toHaveBeenCalled();
  });
});

describe("bid HTTP API", () => {
  function postBid(
    create: BidServicePort["create"],
    payload: Record<string, unknown> = validBidPayload,
    authentication = authenticate,
  ) {
    const app = makeApp(makeAuctions(), authentication, makeBids({ create }));
    return app.inject({
      method: "POST",
      url: "/v1/auctions/91/bids",
      headers: { authorization: "Bearer access-token" },
      payload,
    });
  }

  it("places a new bid and returns 201 with the bid and updated auction", async () => {
    const create = vi.fn<BidServicePort["create"]>(async () => ({
      kind: "placed",
      bid,
      auction: bidAuctionState,
      replay: false,
    }));

    const response = await postBid(create);

    expect(response.statusCode).toBe(201);
    expect(response.json()).toEqual({
      bid: {
        ...bid,
        createdAt: bid.createdAt.toISOString(),
      },
      auction: {
        ...bidAuctionState,
        updatedAt: bidAuctionState.updatedAt.toISOString(),
      },
    });
    expect(create).toHaveBeenCalledWith("91", "42", validBidPayload);
  });

  it("returns 200 for an idempotent replay instead of 201", async () => {
    const create = vi.fn<BidServicePort["create"]>(async () => ({
      kind: "placed",
      bid,
      auction: bidAuctionState,
      replay: true,
    }));

    const response = await postBid(create);

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({ bid: { id: bid.id } });
  });

  it("rejects malformed bid input with 400 INVALID_BID", async () => {
    const create = vi.fn<BidServicePort["create"]>(async () => ({
      kind: "invalid",
      message: "amountMinor must be a positive integer within the supported range",
    }));

    const response = await postBid(create);

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({
      code: "INVALID_BID",
      message: "amountMinor must be a positive integer within the supported range",
    });
  });

  it("returns 404 AUCTION_NOT_FOUND", async () => {
    const create = vi.fn<BidServicePort["create"]>(async () => ({ kind: "auction_not_found" }));

    const response = await postBid(create);

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({
      code: "AUCTION_NOT_FOUND",
      message: "Auction was not found",
    });
  });

  it("returns 409 AUCTION_NOT_OPEN with the current phase", async () => {
    const create = vi.fn<BidServicePort["create"]>(async () => ({
      kind: "auction_not_open",
      phase: "closed",
    }));

    const response = await postBid(create);

    expect(response.statusCode).toBe(409);
    expect(response.json()).toEqual({
      code: "AUCTION_NOT_OPEN",
      message: "Auction is not currently open for bidding",
      phase: "closed",
    });
  });

  it("returns 403 SELF_BID_NOT_ALLOWED", async () => {
    const create = vi.fn<BidServicePort["create"]>(async () => ({ kind: "self_bid_not_allowed" }));

    const response = await postBid(create);

    expect(response.statusCode).toBe(403);
    expect(response.json()).toEqual({
      code: "SELF_BID_NOT_ALLOWED",
      message: "The auction owner cannot bid on their own auction",
    });
  });

  it("returns 422 BID_TOO_LOW with the minimum qualifying amount", async () => {
    const create = vi.fn<BidServicePort["create"]>(async () => ({
      kind: "bid_too_low",
      minimumQualifyingAmountMinor: "1200",
    }));

    const response = await postBid(create);

    expect(response.statusCode).toBe(422);
    expect(response.json()).toEqual({
      code: "BID_TOO_LOW",
      message: "Bid does not meet the minimum qualifying amount",
      minimumQualifyingAmountMinor: "1200",
    });
  });

  it("returns 409 IDEMPOTENCY_KEY_REUSED when the key is reused with a different amount", async () => {
    const create = vi.fn<BidServicePort["create"]>(async () => ({
      kind: "idempotency_key_reused",
      originalAmountMinor: "1100",
    }));

    const response = await postBid(create);

    expect(response.statusCode).toBe(409);
    expect(response.json()).toEqual({
      code: "IDEMPOTENCY_KEY_REUSED",
      message: "This idempotency key was already used with a different amount",
      originalAmountMinor: "1100",
    });
  });

  it("rejects bidding when authentication fails", async () => {
    const create = vi.fn<BidServicePort["create"]>(async () => ({
      kind: "placed",
      bid,
      auction: bidAuctionState,
      replay: false,
    }));
    const deny: preHandlerAsyncHookHandler = async (_request, reply) =>
      reply.code(401).send({ code: "UNAUTHORIZED", message: "Authentication is required" });

    const response = await postBid(create, validBidPayload, deny);

    expect(response.statusCode).toBe(401);
    expect(create).not.toHaveBeenCalled();
  });

  it.each(["not-an-id", "0", "9223372036854775808"])(
    "rejects auction id %j before invoking the service",
    async (id) => {
      const create = vi.fn<BidServicePort["create"]>();
      const app = makeApp(makeAuctions(), authenticate, makeBids({ create }));

      const response = await app.inject({
        method: "POST",
        url: `/v1/auctions/${id}/bids`,
        headers: { authorization: "Bearer access-token" },
        payload: validBidPayload,
      });

      expect(response.statusCode).toBe(400);
      expect(response.json()).toEqual({
        code: "INVALID_REQUEST",
        message: "Request validation failed",
      });
      expect(create).not.toHaveBeenCalled();
    },
  );
});
