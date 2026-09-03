import { registerApi } from "@auction-bonanza/http-routing";
import Fastify, { type FastifyInstance } from "fastify";

import type { IdentityApiDependencies } from "./api/dependencies.js";
import { createApi } from "./api/index.js";

export function buildApp(dependencies: IdentityApiDependencies): FastifyInstance {
  const app = Fastify({ logger: true });

  registerApi(app, createApi(dependencies));

  return app;
}
