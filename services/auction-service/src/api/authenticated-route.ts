import type { AuthenticatedPrincipal } from "@auction-bonanza/access-auth";
import type { ApiRoute } from "@auction-bonanza/http-routing";
import type {
  FastifyReply,
  FastifyRequest,
  FastifySchema,
  preHandlerAsyncHookHandler,
} from "fastify";

interface AuthenticatedRouteOptions {
  authenticate: preHandlerAsyncHookHandler;
  schema: FastifySchema;
  handler: (
    request: FastifyRequest,
    reply: FastifyReply,
    context: { principal: AuthenticatedPrincipal },
  ) => Promise<unknown>;
}

export function authenticatedRoute(options: AuthenticatedRouteOptions): ApiRoute {
  return {
    preHandler: options.authenticate,
    schema: options.schema,
    handler: async (request, reply) => {
      // Fastify does not propagate authentication-hook narrowing into handler types.
      const principal = request.auctionPrincipal;
      if (principal === null) {
        throw new Error("Authenticated auction route has no principal");
      }
      return options.handler(request, reply, { principal });
    },
  };
}
