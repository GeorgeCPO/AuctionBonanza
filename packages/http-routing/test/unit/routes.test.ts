import Fastify from "fastify";
import { afterEach, describe, expect, it } from "vitest";

import { registerApi } from "../../src/index.js";

const apps: ReturnType<typeof Fastify>[] = [];

afterEach(async () => {
  await Promise.all(apps.splice(0).map(async (app) => app.close()));
});

describe("registerApi", () => {
  it("turns nested keys into path segments until it reaches a method", async () => {
    const app = Fastify();
    apps.push(app);

    registerApi(app, {
      v1: {
        users: {
          reset_password: {
            post: async () => ({ reset: true }),
          },
        },
      },
    });

    const response = await app.inject({ method: "POST", url: "/v1/users/reset_password" });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ reset: true });
  });

  it("passes colon-prefixed segments to Fastify as path parameters", async () => {
    const app = Fastify();
    apps.push(app);

    registerApi(app, {
      v1: {
        users: {
          ":userId": {
            bids: {
              ":bidId": {
                get: async (request) => request.params,
              },
            },
          },
        },
      },
    });

    const response = await app.inject({ method: "GET", url: "/v1/users/42/bids/99" });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ userId: "42", bidId: "99" });
  });

  it("registers route-level pre-handler hooks", async () => {
    const app = Fastify();
    apps.push(app);

    registerApi(app, {
      v1: {
        auctions: {
          post: {
            preHandler: async (_request, reply) => reply.code(401).send({ code: "UNAUTHORIZED" }),
            handler: async () => ({ created: true }),
          },
        },
      },
    });

    const response = await app.inject({ method: "POST", url: "/v1/auctions" });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({ code: "UNAUTHORIZED" });
  });
});
