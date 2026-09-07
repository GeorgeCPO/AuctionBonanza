import { randomUUID } from "node:crypto";

import type { IdentityConfig } from "../config.js";
import type { AccessTokens } from "../security/access-tokens.js";
import type { PasswordSecurityPort } from "../security/password-security.js";
import { RefreshTokenSecurity } from "../security/refresh-token-security.js";
import type { UserIdentity, UserRepository } from "../users/user.repository.js";
import type { CurrentUserSession, SessionRepository } from "./session.repository.js";

export interface SessionCredentials {
  accessToken: string;
  refreshToken: string;
  refreshExpiresAt: string;
}

export type LoginOutcome =
  | { kind: "success"; credentials: SessionCredentials; user: UserIdentity }
  | { kind: "invalid" };

export type RefreshOutcome =
  | { kind: "success"; credentials: SessionCredentials }
  | { kind: "invalid" }
  | { kind: "replay"; sessionId: string; userId: string };

export class SessionService {
  readonly #refreshTokens = new RefreshTokenSecurity();

  constructor(
    private readonly config: IdentityConfig,
    private readonly users: UserRepository,
    private readonly sessions: SessionRepository,
    private readonly passwordSecurity: PasswordSecurityPort,
    private readonly accessTokens: AccessTokens,
  ) {}

  async login(email: string, password: string): Promise<LoginOutcome> {
    const user = await this.users.findLoginIdentityByEmail(email.trim().toLowerCase());
    if (user === undefined) {
      await this.passwordSecurity.verifyUnknownUser(password);
      return { kind: "invalid" };
    }

    if (!(await this.passwordSecurity.verify(password, user.passwordHash))) {
      return { kind: "invalid" };
    }

    const credentials = await this.create(user.id);
    return {
      kind: "success",
      credentials,
      user: { id: user.id, email: user.email, username: user.username },
    };
  }

  async create(userId: string): Promise<SessionCredentials> {
    const sessionId = randomUUID();
    const refreshTokenId = randomUUID();
    const refreshToken = this.#refreshTokens.generate();
    const refreshTokenDigest = this.#refreshTokens.digest(refreshToken);
    const accessToken = await this.accessTokens.issue(userId, sessionId);
    const expiresAt = await this.sessions.create({
      sessionId,
      userId,
      refreshTokenId,
      refreshTokenDigest,
      ttlSeconds: this.config.refreshSessionTtlSeconds,
    });

    return {
      accessToken,
      refreshToken,
      refreshExpiresAt: expiresAt.toISOString(),
    };
  }

  async rotate(refreshToken: string): Promise<RefreshOutcome> {
    if (!this.#refreshTokens.isWellFormed(refreshToken)) {
      return { kind: "invalid" };
    }

    const replacementToken = this.#refreshTokens.generate();
    const outcome = await this.sessions.rotate(
      {
        presentedDigest: this.#refreshTokens.digest(refreshToken),
        replacementTokenId: randomUUID(),
        replacementDigest: this.#refreshTokens.digest(replacementToken),
      },
      async ({ userId, sessionId }) => this.accessTokens.issue(userId, sessionId),
    );

    if (outcome.kind !== "success") {
      return outcome;
    }

    return {
      kind: "success",
      credentials: {
        accessToken: outcome.prepared,
        refreshToken: replacementToken,
        refreshExpiresAt: outcome.expiresAt.toISOString(),
      },
    };
  }

  async revoke(refreshToken: string): Promise<void> {
    if (!this.#refreshTokens.isWellFormed(refreshToken)) {
      return;
    }

    await this.sessions.revokeByRefreshTokenDigest(this.#refreshTokens.digest(refreshToken));
  }

  async findCurrent(userId: string, sessionId: string): Promise<CurrentUserSession | undefined> {
    return this.sessions.findCurrent(userId, sessionId);
  }
}
