import type { ApiRoute } from "@auction-bonanza/http-routing";

import type { AccessTokens } from "../security/access-tokens.js";

export function createJwksRoute(accessTokens: AccessTokens): ApiRoute {
  return {
    handler: async (_request, reply) =>
      reply.header("Cache-Control", "public, max-age=300").send(accessTokens.publicJwks()),
  };
}
