import { Type } from "@sinclair/typebox";
import { parseBearerAuthorization } from "@auction-bonanza/access-auth";
import type { ApiRoute } from "@auction-bonanza/http-routing";

import { sendAuthenticationError } from "../api/authentication-error.js";
import type { IdentityRouteDependencies } from "../api/dependencies.js";

const loginBodySchema = Type.Object(
  {
    email: Type.String({
      minLength: 3,
      maxLength: 320,
      pattern: "^\\s*[^\\s@]+@[^\\s@]+\\.[^\\s@]+\\s*$",
    }),
    password: Type.String({ minLength: 1, maxLength: 1_024 }),
  },
  { additionalProperties: false },
);

const refreshTokenBodySchema = Type.Object(
  { refreshToken: Type.String({ minLength: 1, maxLength: 4_096 }) },
  { additionalProperties: false },
);

interface LoginBody {
  email: string;
  password: string;
}

interface RefreshBody {
  refreshToken: string;
}

export class SessionController {
  constructor(private readonly dependencies: IdentityRouteDependencies) {}

  readonly login: ApiRoute = {
    schema: { body: loginBodySchema },
    handler: async (request, reply) => {
      const body = request.body as LoginBody;
      const outcome = await this.dependencies.sessions.login(body.email, body.password);
      if (outcome.kind === "invalid") {
        request.log.warn({ outcome: "invalid_credentials" }, "login rejected");
        return sendAuthenticationError(reply);
      }

      request.log.info({ outcome: "success", userId: outcome.user.id }, "login accepted");
      return reply.code(201).send({
        ...outcome.credentials,
        tokenType: "Bearer",
        expiresIn: this.dependencies.config.accessTokenTtlSeconds,
        user: outcome.user,
      });
    },
  } satisfies ApiRoute;

  readonly refresh: ApiRoute = {
    schema: { body: refreshTokenBodySchema },
    handler: async (request, reply) => {
      const body = request.body as RefreshBody;
      const outcome = await this.dependencies.sessions.rotate(body.refreshToken);
      if (outcome.kind === "success") {
        request.log.info({ outcome: "success" }, "refresh accepted");
        return reply.code(201).send({
          ...outcome.credentials,
          tokenType: "Bearer",
          expiresIn: this.dependencies.config.accessTokenTtlSeconds,
        });
      }

      if (outcome.kind === "replay") {
        request.log.warn(
          {
            outcome: "replay_detected",
            sessionId: outcome.sessionId,
            userId: outcome.userId,
          },
          "refresh rejected and session revoked",
        );
      } else {
        request.log.warn({ outcome: "invalid_refresh" }, "refresh rejected");
      }
      return sendAuthenticationError(reply);
    },
  } satisfies ApiRoute;

  readonly logout: ApiRoute = {
    schema: { body: refreshTokenBodySchema },
    handler: async (request, reply) => {
      const body = request.body as RefreshBody;
      await this.dependencies.sessions.revoke(body.refreshToken);
      return reply.code(204).send();
    },
  } satisfies ApiRoute;

  readonly current: ApiRoute = {
    handler: async (request, reply) => {
      let principal;
      try {
        const token = parseBearerAuthorization(request.headers.authorization);
        principal = await this.dependencies.accessTokens.verify(token);
      } catch {
        return sendAuthenticationError(reply);
      }

      const current = await this.dependencies.sessions.findCurrent(
        principal.userId,
        principal.sessionId,
      );
      if (current === undefined) {
        return sendAuthenticationError(reply);
      }

      return {
        user: current.user,
        session: {
          id: current.session.id,
          createdAt: current.session.createdAt.toISOString(),
          expiresAt: current.session.expiresAt.toISOString(),
          lastRefreshedAt: current.session.lastRefreshedAt.toISOString(),
          revokedAt: current.session.revokedAt?.toISOString() ?? null,
        },
      };
    },
  } satisfies ApiRoute;
}
