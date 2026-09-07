import { registerApi } from "@auction-bonanza/http-routing";
import Fastify, { type FastifyInstance, type FastifyServerOptions } from "fastify";

import type { IdentityApiDependencies } from "./api/dependencies.js";
import { createApi } from "./api/index.js";
import { PostgresSessionRepository } from "./sessions/session.repository.js";
import { SessionService } from "./sessions/session.service.js";
import { PostgresUserRepository } from "./users/user.repository.js";
import { UserService } from "./users/user.service.js";

export const identityLoggerOptions: NonNullable<FastifyServerOptions["logger"]> = {
  redact: {
    paths: [
      "req.headers.authorization",
      "request.headers.authorization",
      "body.password",
      "body.accessToken",
      "body.refreshToken",
      "password",
      "passwordHash",
      "password_hash",
      "accessToken",
      "refreshToken",
      "tokenDigest",
      "token_digest",
      "privateKey",
      "private_key",
    ],
    censor: "[REDACTED]",
  },
};

export function buildApp(dependencies: IdentityApiDependencies): FastifyInstance {
  const users = new PostgresUserRepository(dependencies.database);
  const sessions = new PostgresSessionRepository(dependencies.database);
  const app = Fastify({
    logger: identityLoggerOptions,
  });

  registerApi(
    app,
    createApi({
      config: dependencies.config,
      database: dependencies.database,
      accessTokens: dependencies.accessTokens,
      users: new UserService(users, dependencies.passwordSecurity),
      sessions: new SessionService(
        dependencies.config,
        users,
        sessions,
        dependencies.passwordSecurity,
        dependencies.accessTokens,
      ),
    }),
  );

  app.setErrorHandler((error, request, reply) => {
    const statusCode =
      typeof error === "object" &&
      error !== null &&
      "statusCode" in error &&
      typeof error.statusCode === "number"
        ? error.statusCode
        : undefined;
    if (statusCode !== undefined && statusCode < 500) {
      return reply.code(statusCode).send({
        code: "INVALID_REQUEST",
        message: "Request validation failed",
      });
    }

    request.log.error({ err: error, outcome: "internal_error" }, "request failed");
    return reply.code(500).send({
      code: "INTERNAL_ERROR",
      message: "An unexpected error occurred",
    });
  });

  return app;
}
