import { afterEach, describe, expect, it, vi } from "vitest";

import { buildApp, identityLoggerOptions } from "../../src/app.js";
import type { IdentityApiDependencies, PasswordSecurityPort } from "../../src/api/dependencies.js";
import type { Database, DatabaseClient } from "../../src/database.js";
import type { AccessTokens } from "../../src/security/access-tokens.js";

const apps: ReturnType<typeof buildApp>[] = [];
const config: IdentityApiDependencies["config"] = {
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

const defaultPasswordSecurity: PasswordSecurityPort = {
  hash: async () => "unused",
  verify: async () => true,
  verifyUnknownUser: async () => undefined,
};

const defaultAccessTokens: AccessTokens = {
  issue: async () => "signed.jwt.value",
  verify: async () => ({ userId: "42", sessionId: "session-1", tokenId: "token-1" }),
  publicJwks: () => ({
    keys: [{ kty: "EC", crv: "P-256", kid: "test-key", alg: "ES256", use: "sig" }],
  }),
};

afterEach(async () => {
  await Promise.all(apps.splice(0).map(async (app) => app.close()));
});

function makeDatabase(
  query: Database["query"],
  clientQuery: DatabaseClient["query"] = vi.fn<DatabaseClient["query"]>(async (text) =>
    text.includes("RETURNING expires_at")
      ? { rows: [{ expires_at: new Date("2026-10-03T12:00:00.000Z") }] }
      : { rows: [] },
  ),
): Database {
  return {
    query,
    connect: async () => ({ query: clientQuery, release: vi.fn<() => void>() }),
  };
}

function startApp(
  database: Database,
  passwordSecurity = defaultPasswordSecurity,
  accessTokens = defaultAccessTokens,
) {
  const app = buildApp({ config, database, passwordSecurity, accessTokens });
  apps.push(app);
  return app;
}

describe("session API", () => {
  it("logs in with normalized email and returns client-neutral credentials without hashes", async () => {
    const query = vi.fn<Database["query"]>(async () => ({
      rows: [
        {
          id: "42",
          email: "buyer@example.com",
          username: "Buyer",
          password_hash: "$argon2id$v=19$stored",
        },
      ],
    }));
    const verify = vi.fn<PasswordSecurityPort["verify"]>(async () => true);
    const app = startApp(makeDatabase(query), { ...defaultPasswordSecurity, verify });

    const response = await app.inject({
      method: "POST",
      url: "/v1/sessions",
      payload: { email: " Buyer@Example.COM ", password: "a strong password" },
    });

    expect(response.statusCode).toBe(201);
    expect(response.json()).toMatchObject({
      accessToken: "signed.jwt.value",
      refreshToken: expect.stringMatching(/^[A-Za-z0-9_-]{43}$/),
      tokenType: "Bearer",
      expiresIn: 600,
      refreshExpiresAt: "2026-10-03T12:00:00.000Z",
      user: { id: "42", email: "buyer@example.com", username: "Buyer" },
    });
    expect(query).toHaveBeenCalledWith(expect.stringContaining("lower(email) = $1"), [
      "buyer@example.com",
    ]);
    expect(verify).toHaveBeenCalledWith("a strong password", "$argon2id$v=19$stored");
    expect(response.body).not.toContain("password_hash");
  });

  it("makes unknown-email and incorrect-password failures externally identical", async () => {
    const unknownWork = vi.fn<PasswordSecurityPort["verifyUnknownUser"]>(async () => undefined);
    const unknownApp = startApp(
      makeDatabase(async () => ({ rows: [] })),
      {
        ...defaultPasswordSecurity,
        verifyUnknownUser: unknownWork,
      },
    );
    const wrongVerify = vi.fn<PasswordSecurityPort["verify"]>(async () => false);
    const wrongApp = startApp(
      makeDatabase(async () => ({
        rows: [{ id: "42", email: "buyer@example.com", username: null, password_hash: "hash" }],
      })),
      { ...defaultPasswordSecurity, verify: wrongVerify },
    );
    const request = {
      method: "POST" as const,
      url: "/v1/sessions",
      payload: { email: "buyer@example.com", password: "wrong password" },
    };

    const [unknown, incorrect] = await Promise.all([
      unknownApp.inject(request),
      wrongApp.inject(request),
    ]);

    expect({ status: unknown.statusCode, body: unknown.json() }).toEqual({
      status: incorrect.statusCode,
      body: incorrect.json(),
    });
    expect(unknown.statusCode).toBe(401);
    expect(unknown.headers["www-authenticate"]).toContain("Bearer");
    expect(unknownWork).toHaveBeenCalledWith("wrong password");
    expect(wrongVerify).toHaveBeenCalledOnce();
  });

  it("rejects invalid login and refresh request schemas before credential work", async () => {
    const query = vi.fn<Database["query"]>(async () => ({ rows: [] }));
    const verifyUnknownUser = vi.fn<PasswordSecurityPort["verifyUnknownUser"]>(
      async () => undefined,
    );
    const app = startApp(makeDatabase(query), {
      ...defaultPasswordSecurity,
      verifyUnknownUser,
    });

    const login = await app.inject({
      method: "POST",
      url: "/v1/sessions",
      payload: { email: "invalid", password: "password", extra: true },
    });
    const refresh = await app.inject({
      method: "POST",
      url: "/v1/sessions/refresh",
      payload: {},
    });

    expect(login.statusCode).toBe(400);
    expect(refresh.statusCode).toBe(400);
    expect(query).not.toHaveBeenCalled();
    expect(verifyUnknownUser).not.toHaveBeenCalled();
  });

  it("returns replacement credentials from the refresh endpoint", async () => {
    const presentedToken = Buffer.alloc(32, 7).toString("base64url");
    const clientQuery = vi.fn<DatabaseClient["query"]>(async (text) => {
      if (text.includes("FOR UPDATE")) {
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
    const app = startApp(
      makeDatabase(async () => ({ rows: [] }), clientQuery),
      defaultPasswordSecurity,
      defaultAccessTokens,
    );

    const response = await app.inject({
      method: "POST",
      url: "/v1/sessions/refresh",
      payload: { refreshToken: presentedToken },
    });

    expect(response.statusCode).toBe(201);
    expect(response.json()).toMatchObject({
      accessToken: "signed.jwt.value",
      refreshToken: expect.stringMatching(/^[A-Za-z0-9_-]{43}$/),
      refreshExpiresAt: "2026-10-03T12:00:00.000Z",
      tokenType: "Bearer",
      expiresIn: 600,
    });
    expect(response.json().refreshToken).not.toBe(presentedToken);
  });

  it("returns the generic authentication error for a malformed refresh token", async () => {
    const app = startApp(makeDatabase(async () => ({ rows: [] })));
    const response = await app.inject({
      method: "POST",
      url: "/v1/sessions/refresh",
      payload: { refreshToken: "malformed" },
    });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({
      code: "INVALID_AUTHENTICATION",
      message: "Authentication failed",
    });
  });

  it("authenticates and returns the current user/session representation", async () => {
    const query = vi.fn<Database["query"]>(async () => ({
      rows: [
        {
          id: "42",
          email: "buyer@example.com",
          username: "Buyer",
          session_id: "session-1",
          created_at: new Date("2026-09-03T12:00:00.000Z"),
          expires_at: new Date("2026-10-03T12:00:00.000Z"),
          last_refreshed_at: new Date("2026-09-03T12:00:00.000Z"),
          revoked_at: null,
        },
      ],
    }));
    const verify = vi.fn<AccessTokens["verify"]>(async (token) =>
      defaultAccessTokens.verify(token),
    );
    const app = startApp(makeDatabase(query), defaultPasswordSecurity, {
      ...defaultAccessTokens,
      verify,
    });

    const response = await app.inject({
      method: "GET",
      url: "/v1/sessions/current",
      headers: { authorization: "Bearer signed.jwt.value" },
    });

    expect(response.statusCode).toBe(200);
    expect(verify).toHaveBeenCalledWith("signed.jwt.value");
    expect(query).toHaveBeenCalledWith(expect.stringContaining("session.id = $1"), [
      "session-1",
      "42",
    ]);
    expect(response.json()).toMatchObject({
      user: { id: "42", email: "buyer@example.com" },
      session: { id: "session-1", revokedAt: null },
    });
  });

  it("rejects missing access authentication with a generic 401", async () => {
    const query = vi.fn<Database["query"]>(async () => ({ rows: [] }));
    const app = startApp(makeDatabase(query));
    const response = await app.inject({ method: "GET", url: "/v1/sessions/current" });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({
      code: "INVALID_AUTHENTICATION",
      message: "Authentication failed",
    });
    expect(query).not.toHaveBeenCalled();
  });

  it("makes logout idempotent without revealing whether a token exists", async () => {
    const query = vi.fn<Database["query"]>(async () => ({ rows: [] }));
    const app = startApp(makeDatabase(query));
    const token = Buffer.alloc(32, 9).toString("base64url");

    const first = await app.inject({
      method: "DELETE",
      url: "/v1/sessions/current",
      payload: { refreshToken: token },
    });
    const second = await app.inject({
      method: "DELETE",
      url: "/v1/sessions/current",
      payload: { refreshToken: token },
    });
    const arbitrary = await app.inject({
      method: "DELETE",
      url: "/v1/sessions/current",
      payload: { refreshToken: "not-a-real-token" },
    });

    expect([first.statusCode, second.statusCode, arbitrary.statusCode]).toEqual([204, 204, 204]);
  });

  it("publishes only the configured public JWKS with cache metadata", async () => {
    const app = startApp(makeDatabase(async () => ({ rows: [] })));
    const response = await app.inject({ method: "GET", url: "/.well-known/jwks.json" });

    expect(response.statusCode).toBe(200);
    expect(response.headers["cache-control"]).toBe("public, max-age=300");
    expect(response.json()).toEqual(defaultAccessTokens.publicJwks());
    expect(response.body).not.toContain('"d"');
  });

  it("configures logger redaction for every credential class", () => {
    const loggerConfiguration = JSON.stringify(identityLoggerOptions);
    for (const sensitiveName of [
      "authorization",
      "password",
      "accessToken",
      "refreshToken",
      "tokenDigest",
      "privateKey",
    ]) {
      expect(loggerConfiguration).toContain(sensitiveName);
    }
  });
});
