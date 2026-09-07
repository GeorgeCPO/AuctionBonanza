import { registerApi } from "@auction-bonanza/http-routing";
import Fastify, { type FastifyInstance, type FastifyServerOptions } from "fastify";

import type { AuctionApiDependencies } from "./api/dependencies.js";
import { createApi } from "./api/index.js";

export const auctionLoggerOptions: NonNullable<FastifyServerOptions["logger"]> = {
  redact: {
    paths: ["req.headers.authorization", "request.headers.authorization"],
    censor: "[REDACTED]",
  },
};

export function buildApp(dependencies: AuctionApiDependencies): FastifyInstance {
  const app = Fastify({
    logger: auctionLoggerOptions,
    ajv: { customOptions: { removeAdditional: false } },
  });
  app.decorateRequest("auctionPrincipal", null);
  registerApi(app, createApi(dependencies));

  app.setErrorHandler((error, request, reply) => {
    if (
      typeof error === "object" &&
      error !== null &&
      "statusCode" in error &&
      typeof error.statusCode === "number" &&
      error.statusCode < 500
    ) {
      return reply.code(error.statusCode).send({
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
