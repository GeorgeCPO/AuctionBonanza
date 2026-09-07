import type { ApiTree } from "@auction-bonanza/http-routing";

import { healthHandler } from "../handlers/health.js";
import { createReadinessHandler } from "../handlers/readiness.js";
import type { IdentityRouteDependencies } from "./dependencies.js";
import { createV1Api } from "./v1.js";
import { createJwksRoute } from "./jwks.js";

export function createApi(dependencies: IdentityRouteDependencies): ApiTree {
  return {
    health: {
      get: healthHandler,
    },
    ready: {
      get: createReadinessHandler(dependencies.database),
    },
    ".well-known": {
      "jwks.json": {
        get: createJwksRoute(dependencies.accessTokens),
      },
    },
    ...createV1Api(dependencies),
  };
}
