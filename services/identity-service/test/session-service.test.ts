import { describe, expect, it, vi } from "vitest";

import type { IdentityConfig } from "../src/config.js";
import type { Database, DatabaseClient, QueryResult } from "../src/database.js";
import type { AccessTokens } from "../src/security/access-tokens.js";
import { PostgresSessionRepository } from "../src/sessions/session.repository.js";
import { SessionService } from "../src/sessions/session.service.js";
import type { UserRepository } from "../src/users/user.repository.js";

const config: IdentityConfig = {
  nodeEnv: "test",
  host: "127.0.0.1",
  port: 3001,
  databaseUrl: "postgresql://test:test@localhost:5432/auction_db",
  authIssuer: "https://identity.test",
  authAudience: "auction-api",
  accessTokenTtlSeconds: 600,
  refreshSessionTtlSeconds: 2_592_000,
  accessTokenClockSkewSeconds: 5,
  signingAlgorithm: "ES256",
  activeSigningKeyId: "test-key",
  signingPrivateKeyPath: "/not-used/private.pem",
  signingJwksPath: "/not-used/jwks.json",
};

const accessTokens: AccessTokens = {
  issue: async () => "signed-access-token",
  verify: async () => ({ userId: "42", sessionId: "session", tokenId: "token" }),
  publicJwks: () => ({ keys: [] }),
};

const unusedUsers: UserRepository = {
  findLoginIdentityByEmail: async () => undefined,
  create: async () => Promise.reject(new Error("unused")),
};

function makeService(database: Database, tokenIssuer: AccessTokens = accessTokens): SessionService {
  return new SessionService(
    config,
    unusedUsers,
    new PostgresSessionRepository(database),
    {
      hash: async () => "unused",
      verify: async () => false,
      verifyUnknownUser: async () => undefined,
    },
    tokenIssuer,
  );
}

function makeTransactionalDatabase(implementation: DatabaseClient["query"]): {
  database: Database;
  release: ReturnType<typeof vi.fn>;
} {
  const release = vi.fn<() => void>();
  const client: DatabaseClient = { query: implementation, release };
  return {
    database: {
      query: vi.fn<Database["query"]>(async () => ({ rows: [] })),
      connect: vi.fn<Database["connect"]>(async () => client),
    },
    release,
  };
}

describe("SessionService", () => {
  it("creates a session while persisting only a 32-byte refresh-token digest", async () => {
    const queries: { text: string; values?: readonly unknown[] }[] = [];
    const { database, release } = makeTransactionalDatabase(async (text, values) => {
      queries.push({ text, ...(values === undefined ? {} : { values }) });
      return text.includes("RETURNING expires_at")
        ? { rows: [{ expires_at: new Date("2026-10-03T12:00:00.000Z") }] }
        : { rows: [] };
    });
    const service = makeService(database);

    const credentials = await service.create("42");

    expect(credentials.accessToken).toBe("signed-access-token");
    expect(credentials.refreshToken).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(credentials.refreshExpiresAt).toBe("2026-10-03T12:00:00.000Z");
    const refreshInsert = queries.find((query) =>
      query.text.includes("INSERT INTO identity_refresh_tokens"),
    );
    expect(refreshInsert?.values?.[2]).toBeInstanceOf(Buffer);
    expect(refreshInsert?.values?.[2]).toHaveLength(32);
    expect(refreshInsert?.values).not.toContain(credentials.refreshToken);
    expect(queries.at(-1)?.text).toBe("COMMIT");
    expect(release).toHaveBeenCalledOnce();
  });

  it("rotates a current refresh token within one explicit transaction", async () => {
    const queries: { text: string; values?: readonly unknown[] }[] = [];
    const { database } = makeTransactionalDatabase(async (text, values) => {
      queries.push({ text, ...(values === undefined ? {} : { values }) });
      if (text.includes("SELECT") && text.includes("FOR UPDATE")) {
        return {
          rows: [
            {
              token_id: "00000000-0000-4000-8000-000000000001",
              session_id: "00000000-0000-4000-8000-000000000002",
              user_id: "42",
              generation: 0,
              consumed_at: null,
              revoked_at: null,
              expired: false,
            },
          ],
        };
      }
      if (text.includes("UPDATE identity_sessions") && text.includes("RETURNING")) {
        return { rows: [{ expires_at: new Date("2026-10-03T12:00:00.000Z") }] };
      }
      return { rows: [] };
    });
    const service = makeService(database);
    const presentedToken = Buffer.alloc(32, 1).toString("base64url");

    const outcome = await service.rotate(presentedToken);

    expect(outcome.kind).toBe("success");
    if (outcome.kind !== "success") throw new Error("expected success");
    expect(outcome.credentials.refreshToken).not.toBe(presentedToken);
    expect(queries.map(({ text }) => text.trim().split(/\s+/).slice(0, 3).join(" "))).toEqual([
      "BEGIN",
      "SELECT token.id::text AS",
      "UPDATE identity_refresh_tokens SET",
      "INSERT INTO identity_refresh_tokens",
      "UPDATE identity_sessions SET",
      "COMMIT",
    ]);
  });

  it.each([
    ["expired", { consumed_at: null, revoked_at: null, expired: true }],
    ["revoked", { consumed_at: null, revoked_at: new Date(), expired: false }],
  ])("rejects an %s session without issuing replacements", async (_label, state) => {
    const query = vi.fn<DatabaseClient["query"]>(async (text): Promise<QueryResult> =>
      text.includes("FOR UPDATE")
        ? {
            rows: [
              {
                token_id: "token-id",
                session_id: "session-id",
                user_id: "42",
                generation: 0,
                ...state,
              },
            ],
          }
        : { rows: [] },
    );
    const { database } = makeTransactionalDatabase(query);
    const service = makeService(database);

    await expect(service.rotate(Buffer.alloc(32, 2).toString("base64url"))).resolves.toEqual({
      kind: "invalid",
    });
    expect(
      query.mock.calls.some(([sql]) => sql.includes("INSERT INTO identity_refresh_tokens")),
    ).toBe(false);
  });

  it("revokes the full family when a consumed token is replayed", async () => {
    const query = vi.fn<DatabaseClient["query"]>(async (text): Promise<QueryResult> =>
      text.includes("FOR UPDATE")
        ? {
            rows: [
              {
                token_id: "token-id",
                session_id: "session-id",
                user_id: "42",
                generation: 0,
                consumed_at: new Date(),
                revoked_at: null,
                expired: false,
              },
            ],
          }
        : { rows: [] },
    );
    const { database } = makeTransactionalDatabase(query);
    const service = makeService(database);

    await expect(service.rotate(Buffer.alloc(32, 3).toString("base64url"))).resolves.toEqual({
      kind: "replay",
      sessionId: "session-id",
      userId: "42",
    });
    expect(
      query.mock.calls.some(
        ([sql]) => sql.includes("UPDATE identity_sessions") && sql.includes("refresh_token_replay"),
      ),
    ).toBe(true);
    expect(query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
  });

  it("rolls back and never returns credentials when rotation fails before commit", async () => {
    const query = vi.fn<DatabaseClient["query"]>(async (text): Promise<QueryResult> => {
      if (text.includes("FOR UPDATE")) throw new Error("database unavailable");
      return { rows: [] };
    });
    const { database, release } = makeTransactionalDatabase(query);
    const service = makeService(database);

    await expect(service.rotate(Buffer.alloc(32, 4).toString("base64url"))).rejects.toThrow(
      "database unavailable",
    );
    expect(query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(release).toHaveBeenCalledOnce();
  });

  it("rolls back without consuming the refresh token when access-token signing fails", async () => {
    const query = vi.fn<DatabaseClient["query"]>(async (text): Promise<QueryResult> =>
      text.includes("FOR UPDATE")
        ? {
            rows: [
              {
                token_id: "token-id",
                session_id: "session-id",
                user_id: "42",
                generation: 0,
                consumed_at: null,
                revoked_at: null,
                expired: false,
              },
            ],
          }
        : { rows: [] },
    );
    const { database } = makeTransactionalDatabase(query);
    const service = makeService(database, {
      ...accessTokens,
      issue: async () => Promise.reject(new Error("signing unavailable")),
    });

    await expect(service.rotate(Buffer.alloc(32, 5).toString("base64url"))).rejects.toThrow(
      "signing unavailable",
    );
    expect(query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(query.mock.calls.some(([sql]) => sql.includes("SET consumed_at = now()"))).toBe(false);
  });
});
