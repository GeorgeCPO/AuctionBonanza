import type { RouteHandlerMethod } from "fastify";

import type { Database } from "../database.js";

export function createReadinessHandler(database: Database): RouteHandlerMethod {
  return async (_request, reply) => {
    try {
      await database.query("SELECT 1");
      return { status: "ready" };
    } catch (error) {
      reply.log.warn({ err: error }, "readiness database check failed");
      return reply.code(503).send({ status: "not_ready" });
    }
  };
}
