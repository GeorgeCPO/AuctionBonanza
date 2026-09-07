import { createPrivateKey, createPublicKey, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import {
  AccessTokenVerifier,
  createStaticJwksKeyResolver,
  type AuthenticatedPrincipal,
} from "@auction-bonanza/access-auth";
import { importPKCS8, SignJWT, type JSONWebKeySet, type JWK } from "jose";

import type { IdentityConfig } from "../config.js";

export interface AccessTokens {
  issue(userId: string, sessionId: string): Promise<string>;
  verify(token: string): Promise<AuthenticatedPrincipal>;
  publicJwks(): JSONWebKeySet;
}

export async function createAccessTokens(config: IdentityConfig): Promise<AccessTokens> {
  const [privateKeyPem, jwksText] = await Promise.all([
    readFile(config.signingPrivateKeyPath, "utf8"),
    readFile(config.signingJwksPath, "utf8"),
  ]);

  let jwks: JSONWebKeySet;
  try {
    jwks = JSON.parse(jwksText) as JSONWebKeySet;
  } catch {
    throw new Error("The configured JWKS file is not valid JSON");
  }

  const activePublicJwk = jwks.keys.find((key) => key.kid === config.activeSigningKeyId);
  if (activePublicJwk === undefined) {
    throw new Error("The active signing key ID is missing from the configured JWKS");
  }

  const derivedPublicJwk = createPublicKey(createPrivateKey(privateKeyPem)).export({
    format: "jwk",
  }) as JWK;
  if (activePublicJwk.x !== derivedPublicJwk.x || activePublicJwk.y !== derivedPublicJwk.y) {
    throw new Error("The active public JWKS key does not match the private signing key");
  }

  const privateKey = await importPKCS8(privateKeyPem, config.signingAlgorithm);
  const keyResolver = await createStaticJwksKeyResolver(jwks);
  const verifier = new AccessTokenVerifier({
    issuer: config.authIssuer,
    audience: config.authAudience,
    algorithm: config.signingAlgorithm,
    accessTokenTtlSeconds: config.accessTokenTtlSeconds,
    clockSkewSeconds: config.accessTokenClockSkewSeconds,
    keyResolver,
  });

  return {
    issue: async (userId, sessionId) =>
      new SignJWT({ sid: sessionId })
        .setProtectedHeader({
          alg: config.signingAlgorithm,
          kid: config.activeSigningKeyId,
          typ: "at+jwt",
        })
        .setIssuer(config.authIssuer)
        .setSubject(userId)
        .setAudience(config.authAudience)
        .setIssuedAt()
        .setExpirationTime(`${config.accessTokenTtlSeconds}s`)
        .setJti(randomUUID())
        .sign(privateKey),
    verify: async (token) => verifier.verify(token),
    publicJwks: () => structuredClone(jwks),
  };
}
