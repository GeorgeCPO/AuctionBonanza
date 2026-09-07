import type { Database, DatabaseClient } from "../database.js";

export interface CreateSessionInput {
  sessionId: string;
  userId: string;
  refreshTokenId: string;
  refreshTokenDigest: Buffer;
  ttlSeconds: number;
}

export interface RotateRefreshTokenInput {
  presentedDigest: Buffer;
  replacementTokenId: string;
  replacementDigest: Buffer;
}

export interface RotationContext {
  sessionId: string;
  userId: string;
}

export type RotationPersistenceOutcome<T> =
  | { kind: "success"; prepared: T; expiresAt: Date }
  | { kind: "invalid" }
  | { kind: "replay"; sessionId: string; userId: string };

export interface CurrentUserSession {
  user: {
    id: string;
    email: string;
    username: string | null;
  };
  session: {
    id: string;
    createdAt: Date;
    expiresAt: Date;
    lastRefreshedAt: Date;
    revokedAt: Date | null;
  };
}

export interface SessionRepository {
  create(input: CreateSessionInput): Promise<Date>;
  /**
   * Runs the complete locked rotation transaction. The callback prepares the
   * success value before the presented token is consumed, so a signing failure
   * rolls the transaction back and leaves that token current.
   */
  rotate<T>(
    input: RotateRefreshTokenInput,
    prepareBeforeCommit: (context: RotationContext) => Promise<T>,
  ): Promise<RotationPersistenceOutcome<T>>;
  revokeByRefreshTokenDigest(digest: Buffer): Promise<void>;
  findCurrent(userId: string, sessionId: string): Promise<CurrentUserSession | undefined>;
}

interface RefreshRow extends Record<string, unknown> {
  token_id: string;
  session_id: string;
  user_id: string;
  generation: number;
  consumed_at: Date | null;
  revoked_at: Date | null;
  expired: boolean;
}

interface SessionExpiryRow extends Record<string, unknown> {
  expires_at: Date;
}

interface CurrentSessionRow extends Record<string, unknown> {
  id: string;
  email: string;
  username: string | null;
  session_id: string;
  created_at: Date;
  expires_at: Date;
  last_refreshed_at: Date;
  revoked_at: Date | null;
}

export class PostgresSessionRepository implements SessionRepository {
  constructor(private readonly database: Database) {}

  async create(input: CreateSessionInput): Promise<Date> {
    const client = await this.database.connect();

    try {
      await client.query("BEGIN");
      const result = await client.query(
        `INSERT INTO identity_sessions (id, user_id, expires_at)
         VALUES ($1, $2, now() + ($3::double precision * interval '1 second'))
         RETURNING expires_at`,
        [input.sessionId, input.userId, input.ttlSeconds],
      );
      const expiry = result.rows[0] as SessionExpiryRow | undefined;
      if (expiry === undefined) {
        throw new Error("Session insert returned no row");
      }

      await client.query(
        `INSERT INTO identity_refresh_tokens
           (id, session_id, parent_token_id, generation, token_digest)
         VALUES ($1, $2, NULL, 0, $3)`,
        [input.refreshTokenId, input.sessionId, input.refreshTokenDigest],
      );
      await client.query("COMMIT");
      return expiry.expires_at;
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async rotate<T>(
    input: RotateRefreshTokenInput,
    prepareBeforeCommit: (context: RotationContext) => Promise<T>,
  ): Promise<RotationPersistenceOutcome<T>> {
    const client = await this.database.connect();
    let committed = false;

    try {
      await client.query("BEGIN");
      const result = await client.query(
        `SELECT
           token.id::text AS token_id,
           token.session_id::text,
           session.user_id::text,
           token.generation,
           token.consumed_at,
           session.revoked_at,
           session.expires_at <= now() AS expired
         FROM identity_refresh_tokens AS token
         JOIN identity_sessions AS session ON session.id = token.session_id
         WHERE token.token_digest = $1
         FOR UPDATE OF token, session`,
        [input.presentedDigest],
      );
      const row = result.rows[0] as RefreshRow | undefined;

      if (row === undefined) {
        await client.query("COMMIT");
        committed = true;
        return { kind: "invalid" };
      }

      if (row.consumed_at !== null) {
        await client.query(
          `UPDATE identity_sessions
           SET revoked_at = COALESCE(revoked_at, now()),
               revocation_reason = COALESCE(revocation_reason, 'refresh_token_replay')
           WHERE id = $1`,
          [row.session_id],
        );
        await client.query("COMMIT");
        committed = true;
        return { kind: "replay", sessionId: row.session_id, userId: row.user_id };
      }

      if (row.revoked_at !== null || row.expired) {
        await client.query("COMMIT");
        committed = true;
        return { kind: "invalid" };
      }

      const prepared = await prepareBeforeCommit({
        sessionId: row.session_id,
        userId: row.user_id,
      });
      await client.query("UPDATE identity_refresh_tokens SET consumed_at = now() WHERE id = $1", [
        row.token_id,
      ]);
      await client.query(
        `INSERT INTO identity_refresh_tokens
           (id, session_id, parent_token_id, generation, token_digest)
         VALUES ($1, $2, $3, $4, $5)`,
        [
          input.replacementTokenId,
          row.session_id,
          row.token_id,
          row.generation + 1,
          input.replacementDigest,
        ],
      );
      const expiryResult = await client.query(
        `UPDATE identity_sessions
         SET last_refreshed_at = now()
         WHERE id = $1
         RETURNING expires_at`,
        [row.session_id],
      );
      const expiry = expiryResult.rows[0] as SessionExpiryRow | undefined;
      if (expiry === undefined) {
        throw new Error("Session rotation returned no row");
      }

      await client.query("COMMIT");
      committed = true;
      return { kind: "success", prepared, expiresAt: expiry.expires_at };
    } catch (error) {
      if (!committed) {
        await rollback(client);
      }
      throw error;
    } finally {
      client.release();
    }
  }

  async revokeByRefreshTokenDigest(digest: Buffer): Promise<void> {
    await this.database.query(
      `UPDATE identity_sessions AS session
       SET revoked_at = COALESCE(session.revoked_at, now()),
           revocation_reason = COALESCE(session.revocation_reason, 'logout')
       FROM identity_refresh_tokens AS token
       WHERE token.token_digest = $1
         AND token.session_id = session.id`,
      [digest],
    );
  }

  async findCurrent(userId: string, sessionId: string): Promise<CurrentUserSession | undefined> {
    const result = await this.database.query(
      `SELECT
         users.id::text,
         users.email,
         users.username,
         session.id::text AS session_id,
         session.created_at,
         session.expires_at,
         session.last_refreshed_at,
         session.revoked_at
       FROM identity_sessions AS session
       JOIN users ON users.id = session.user_id
       WHERE session.id = $1 AND users.id = $2`,
      [sessionId, userId],
    );
    const row = result.rows[0] as CurrentSessionRow | undefined;

    return row === undefined
      ? undefined
      : {
          user: { id: row.id, email: row.email, username: row.username },
          session: {
            id: row.session_id,
            createdAt: row.created_at,
            expiresAt: row.expires_at,
            lastRefreshedAt: row.last_refreshed_at,
            revokedAt: row.revoked_at,
          },
        };
  }
}

async function rollback(client: DatabaseClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch {
    // Preserve the original transaction error; the checked-out client is
    // released immediately and pg will discard it if the connection failed.
  }
}
