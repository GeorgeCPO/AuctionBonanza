import {
  AccessTokenVerifier,
  createRemoteJwksKeyResolver,
  parseBearerAuthorization,
  type AuthenticatedPrincipal,
} from "@auction-bonanza/access-auth";
import type { preHandlerAsyncHookHandler } from "fastify";

import { isPostgresBigintId } from "./auctions/auction.service.js";

declare module "fastify" {
  interface FastifyRequest {
    auctionPrincipal: AuthenticatedPrincipal | null;
  }
}

export interface AuctionAccessAuthenticationConfig {
  issuer: string;
  audience: string;
  jwksUrl: string;
  accessTokenTtlSeconds: number;
  clockSkewSeconds: number;
  jwksCacheTtlMilliseconds: number;
}

export function createAuctionAccessTokenVerifier(
  config: AuctionAccessAuthenticationConfig,
): AccessTokenVerifier {
  return new AccessTokenVerifier({
    issuer: config.issuer,
    audience: config.audience,
    algorithm: "ES256",
    accessTokenTtlSeconds: config.accessTokenTtlSeconds,
    clockSkewSeconds: config.clockSkewSeconds,
    keyResolver: createRemoteJwksKeyResolver({
      jwksUrl: config.jwksUrl,
      cacheTtlMs: config.jwksCacheTtlMilliseconds,
      maximumKeys: 8,
    }),
  });
}

export function createAuthenticationHook(
  verifier: Pick<AccessTokenVerifier, "verify">,
): preHandlerAsyncHookHandler {
  return async (request, reply) => {
    try {
      const token = parseBearerAuthorization(request.headers.authorization);
      const principal = await verifier.verify(token);
      if (!isPostgresBigintId(principal.userId)) {
        throw new Error("Access token subject is not an auction user ID");
      }
      request.auctionPrincipal = principal;
    } catch {
      return reply.code(401).send({
        code: "UNAUTHORIZED",
        message: "Authentication is required",
      });
    }
  };
}
