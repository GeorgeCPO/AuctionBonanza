import type { RouteHandlerMethod } from "fastify";

export const healthHandler: RouteHandlerMethod = async () => ({ status: "ok" });
