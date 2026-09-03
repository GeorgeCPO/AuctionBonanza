import type { ApiTree } from "@auction-bonanza/http-routing";

import { healthHandler } from "../handlers/health.js";
import { createReadinessHandler } from "../handlers/readiness.js";
import type { IdentityApiDependencies } from "./dependencies.js";
import { createV1Api } from "./v1.js";

export function createApi(dependencies: IdentityApiDependencies): ApiTree {
  return {
    health: {
      get: healthHandler,
    },
    ready: {
      get: createReadinessHandler(dependencies.database),
    },
    ...createV1Api(dependencies),
  };
}
