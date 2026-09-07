import { AccessTokenVerifier, createRemoteJwksKeyResolver } from "@auction-bonanza/access-auth";

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
