import { randomUUID } from "node:crypto";

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import type { IdentityConfig } from "../../src/config.js";
import { createDatabasePool, type DatabasePool } from "../../src/database.js";
import type { AccessTokens } from "../../src/security/access-tokens.js";
import { RefreshTokenSecurity } from "../../src/security/refresh-token-security.js";
import { PostgresSessionRepository } from "../../src/sessions/session.repository.js";
import { SessionService } from "../../src/sessions/session.service.js";
import type { UserRepository } from "../../src/users/user.repository.js";
import { requireEnvironment } from "../support/environment.js";

const databaseUrl = requireEnvironment("TEST_DATABASE_URL");

describe("refresh-session PostgreSQL behavior", () => {
  let database: DatabasePool;
  let userId: string;
  const config: IdentityConfig = {
    nodeEnv: "test",
    host: "127.0.0.1",
    port: 3001,
    databaseUrl,
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
    issue: async (subject, sessionId) => `access:${subject}:${sessionId}:${randomUUID()}`,
    verify: async () => Promise.reject(new Error("unused")),
    publicJwks: () => ({ keys: [] }),
  };
  const unusedUsers: UserRepository = {
    findLoginIdentityByEmail: async () => undefined,
    create: async () => Promise.reject(new Error("unused")),
  };

  function makeService(): SessionService {
    return new SessionService(
      config,
      unusedUsers,
      new PostgresSessionRepository(database),
      {
        hash: async () => "unused",
        verify: async () => false,
        verifyUnknownUser: async () => undefined,
      },
      accessTokens,
    );
  }

  beforeAll(() => {
    database = createDatabasePool(config);
  });

  beforeEach(async () => {
    const result = await database.query(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'Integration User', '$argon2id$test')
       RETURNING id::text`,
      [`auth-integration-${randomUUID()}@example.com`],
    );
    userId = result.rows[0]?.id as string;
  });

  afterEach(async () => {
    await database.query("DELETE FROM identity_sessions WHERE user_id = $1", [userId]);
    await database.query("DELETE FROM users WHERE id = $1", [userId]);
  });

  afterAll(async () => {
    await database.end();
  });

  it("stores only a SHA-256 digest and rotates the current token", async () => {
    const service = makeService();
    const initial = await service.create(userId);
    const before = await database.query(
      `SELECT token_digest, consumed_at, generation
       FROM identity_refresh_tokens
       ORDER BY generation`,
    );

    expect(before.rows).toHaveLength(1);
    expect(before.rows[0]?.token_digest).toEqual(
      new RefreshTokenSecurity().digest(initial.refreshToken),
    );
    expect(JSON.stringify(before.rows)).not.toContain(initial.refreshToken);

    const rotated = await service.rotate(initial.refreshToken);
    expect(rotated.kind).toBe("success");
    const after = await database.query(
      `SELECT octet_length(token_digest) AS digest_length, consumed_at, generation
       FROM identity_refresh_tokens
       ORDER BY generation`,
    );
    expect(after.rows).toHaveLength(2);
    expect(after.rows.map((row) => row.digest_length)).toEqual([32, 32]);
    expect(after.rows[0]?.consumed_at).toBeInstanceOf(Date);
    expect(after.rows[1]?.consumed_at).toBeNull();
  });

  it("rejects expired and revoked refresh sessions", async () => {
    const service = makeService();
    const expired = await service.create(userId);
    await database.query(
      `UPDATE identity_sessions
       SET created_at = now() - interval '2 days',
           last_refreshed_at = now() - interval '2 days',
           expires_at = now() - interval '1 day'
       WHERE user_id = $1`,
      [userId],
    );
    await expect(service.rotate(expired.refreshToken)).resolves.toEqual({ kind: "invalid" });

    await database.query("DELETE FROM identity_sessions WHERE user_id = $1", [userId]);
    const revoked = await service.create(userId);
    await database.query(
      `UPDATE identity_sessions
       SET revoked_at = now(), revocation_reason = 'test'
       WHERE user_id = $1`,
      [userId],
    );
    await expect(service.rotate(revoked.refreshToken)).resolves.toEqual({ kind: "invalid" });
  });

  it("detects replay and revokes the whole session family", async () => {
    const service = makeService();
    const initial = await service.create(userId);
    const first = await service.rotate(initial.refreshToken);
    expect(first.kind).toBe("success");

    const replay = await service.rotate(initial.refreshToken);
    expect(replay.kind).toBe("replay");
    const session = await database.query(
      "SELECT revoked_at, revocation_reason FROM identity_sessions WHERE user_id = $1",
      [userId],
    );
    expect(session.rows[0]?.revoked_at).toBeInstanceOf(Date);
    expect(session.rows[0]?.revocation_reason).toBe("refresh_token_replay");
    if (first.kind !== "success") {
      throw new Error("expected first rotation to succeed");
    }
    await expect(service.rotate(first.credentials.refreshToken)).resolves.toEqual({
      kind: "invalid",
    });
  });

  it("serializes two refreshes so no more than one rotation succeeds", async () => {
    const service = makeService();
    const initial = await service.create(userId);

    const outcomes = await Promise.all([
      service.rotate(initial.refreshToken),
      service.rotate(initial.refreshToken),
    ]);

    expect(outcomes.filter(({ kind }) => kind === "success")).toHaveLength(1);
    expect(outcomes.filter(({ kind }) => kind === "replay")).toHaveLength(1);
    const session = await database.query(
      "SELECT revoked_at, revocation_reason FROM identity_sessions WHERE user_id = $1",
      [userId],
    );
    expect(session.rows[0]?.revoked_at).toBeInstanceOf(Date);
    expect(session.rows[0]?.revocation_reason).toBe("refresh_token_replay");
  });

  it("revokes logout idempotently without token-existence feedback", async () => {
    const service = makeService();
    const credentials = await service.create(userId);

    await expect(service.revoke(credentials.refreshToken)).resolves.toBeUndefined();
    await expect(service.revoke(credentials.refreshToken)).resolves.toBeUndefined();
    await expect(
      service.revoke(Buffer.alloc(32, 8).toString("base64url")),
    ).resolves.toBeUndefined();
    const session = await database.query(
      "SELECT revoked_at, revocation_reason FROM identity_sessions WHERE user_id = $1",
      [userId],
    );
    expect(session.rows[0]?.revoked_at).toBeInstanceOf(Date);
    expect(session.rows[0]?.revocation_reason).toBe("logout");
  });
});
