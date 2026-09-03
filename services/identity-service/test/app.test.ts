import { afterEach, describe, expect, it, vi } from "vitest";

import { buildApp } from "../src/app.js";
import type { IdentityApiDependencies } from "../src/api/dependencies.js";
import type { Database } from "../src/database.js";

const apps: ReturnType<typeof buildApp>[] = [];
const config: IdentityApiDependencies["config"] = {
  nodeEnv: "test",
  host: "127.0.0.1",
  port: 3001,
  databaseUrl: "postgresql://test:test@localhost:5432/auction_db",
};

const unusedPasswordSecurity = {
  hash: async (): Promise<string> => "unused",
};

afterEach(async () => {
  await Promise.all(apps.splice(0).map(async (app) => app.close()));
});

function makeDatabase(
  query: Database["query"] = vi.fn<Database["query"]>(async () => ({ rows: [] })),
): Database {
  return { query };
}

describe("identity service", () => {
  it("reports process health without querying PostgreSQL", async () => {
    const query = vi.fn<Database["query"]>(async () => ({ rows: [] }));
    const database = makeDatabase(query);
    const app = buildApp({ config, database, passwordSecurity: unusedPasswordSecurity });
    apps.push(app);

    const response = await app.inject({ method: "GET", url: "/health" });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ status: "ok" });
    expect(query).not.toHaveBeenCalled();
  });

  it("reports readiness only when PostgreSQL responds", async () => {
    const query = vi.fn<Database["query"]>(async () => ({ rows: [{ "?column?": 1 }] }));
    const database = makeDatabase(query);
    const app = buildApp({ config, database, passwordSecurity: unusedPasswordSecurity });
    apps.push(app);

    const response = await app.inject({ method: "GET", url: "/ready" });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ status: "ready" });
    expect(query).toHaveBeenCalledWith("SELECT 1");
  });

  it("returns 503 when PostgreSQL is unavailable", async () => {
    const database = makeDatabase(
      vi.fn<Database["query"]>(async () => Promise.reject(new Error("offline"))),
    );
    const app = buildApp({ config, database, passwordSecurity: unusedPasswordSecurity });
    apps.push(app);

    const response = await app.inject({ method: "GET", url: "/ready" });

    expect(response.statusCode).toBe(503);
    expect(response.json()).toEqual({ status: "not_ready" });
  });

  it("hashes a password and inserts a normalized user with parameters", async () => {
    const query = vi.fn<Database["query"]>(async () => ({
      rows: [
        {
          id: "42",
          email: "buyer@example.com",
          username: "Buyer",
          created_at: new Date("2026-09-02T12:00:00.000Z"),
        },
      ],
    }));
    const hash = vi.fn<IdentityApiDependencies["passwordSecurity"]["hash"]>(
      async () => "$argon2id$test-hash",
    );
    const app = buildApp({
      config,
      database: makeDatabase(query),
      passwordSecurity: { hash },
    });
    apps.push(app);

    const response = await app.inject({
      method: "POST",
      url: "/v1/users",
      payload: {
        email: " Buyer@Example.COM ",
        password: "a strong password",
        username: " Buyer ",
      },
    });

    expect(response.statusCode).toBe(201);
    expect(response.json()).toEqual({
      id: "42",
      email: "buyer@example.com",
      username: "Buyer",
      createdAt: "2026-09-02T12:00:00.000Z",
    });
    expect(hash).toHaveBeenCalledWith("a strong password");
    expect(query).toHaveBeenCalledWith(expect.stringContaining("VALUES ($1, $2, $3)"), [
      "buyer@example.com",
      "Buyer",
      "$argon2id$test-hash",
    ]);
    expect(response.body).not.toContain("password");
  });

  it("rejects invalid registration input before hashing or querying", async () => {
    const query = vi.fn<Database["query"]>(async () => ({ rows: [] }));
    const database = makeDatabase(query);
    const hash = vi.fn<IdentityApiDependencies["passwordSecurity"]["hash"]>(async () => "unused");
    const app = buildApp({ config, database, passwordSecurity: { hash } });
    apps.push(app);

    const response = await app.inject({
      method: "POST",
      url: "/v1/users",
      payload: { email: "not-an-email", password: "short" },
    });

    expect(response.statusCode).toBe(400);
    expect(hash).not.toHaveBeenCalled();
    expect(query).not.toHaveBeenCalled();
  });
});
