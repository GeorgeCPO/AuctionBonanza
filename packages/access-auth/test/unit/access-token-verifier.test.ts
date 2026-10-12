import { generateKeyPair, exportJWK, SignJWT, type JWK } from "jose";
import { beforeAll, describe, expect, it, vi } from "vitest";

import {
  AccessAuthenticationError,
  AccessTokenVerifier,
  createRemoteJwksKeyResolver,
  createStaticJwksKeyResolver,
  parseBearerAuthorization,
} from "../../src/index.js";

const NOW_SECONDS = 1_800_000_000;
const ISSUER = "https://identity.test";
const AUDIENCE = "auction-api";

let privateKey: Awaited<ReturnType<typeof generateKeyPair>>["privateKey"];
let publicJwk: JWK;

beforeAll(async () => {
  const pair = await generateKeyPair("ES256");
  privateKey = pair.privateKey;
  publicJwk = {
    ...(await exportJWK(pair.publicKey)),
    alg: "ES256",
    kid: "key-1",
    use: "sig",
  };
});

async function signToken(
  changes: {
    alg?: "ES256" | "HS256";
    audience?: string;
    expiresAt?: number;
    issuedAt?: number;
    issuer?: string;
    kid?: string;
    payload?: Record<string, unknown>;
    type?: string;
  } = {},
): Promise<string> {
  const algorithm = changes.alg ?? "ES256";
  const key = algorithm === "ES256" ? privateKey : new TextEncoder().encode("a".repeat(32));
  return new SignJWT({ sid: "session-1", ...changes.payload })
    .setProtectedHeader({
      alg: algorithm,
      kid: changes.kid ?? "key-1",
      typ: changes.type ?? "at+jwt",
    })
    .setIssuer(changes.issuer ?? ISSUER)
    .setSubject("42")
    .setAudience(changes.audience ?? AUDIENCE)
    .setIssuedAt(changes.issuedAt ?? NOW_SECONDS)
    .setExpirationTime(changes.expiresAt ?? NOW_SECONDS + 600)
    .setJti("token-1")
    .sign(key);
}

async function makeVerifier(jwks = { keys: [publicJwk] }): Promise<AccessTokenVerifier> {
  return new AccessTokenVerifier({
    issuer: ISSUER,
    audience: AUDIENCE,
    algorithm: "ES256",
    accessTokenTtlSeconds: 600,
    clockSkewSeconds: 5,
    keyResolver: await createStaticJwksKeyResolver(jwks),
    now: () => NOW_SECONDS * 1_000,
  });
}

describe("bearer parsing", () => {
  it("accepts one bearer credential and rejects missing or ambiguous values", () => {
    expect(parseBearerAuthorization("Bearer abc.def.ghi")).toBe("abc.def.ghi");
    expect(() => parseBearerAuthorization(undefined)).toThrow(AccessAuthenticationError);
    expect(() => parseBearerAuthorization("Basic abc")).toThrow(AccessAuthenticationError);
    expect(() => parseBearerAuthorization("Bearer one, Bearer two")).toThrow(
      AccessAuthenticationError,
    );
  });
});

describe("AccessTokenVerifier", () => {
  it("returns the minimal authenticated principal for a valid token", async () => {
    const verifier = await makeVerifier();
    await expect(verifier.verify(await signToken())).resolves.toEqual({
      userId: "42",
      sessionId: "session-1",
      tokenId: "token-1",
    });
  });

  it("rejects a tampered signature", async () => {
    const verifier = await makeVerifier();
    const token = await signToken();
    const [protectedHeader, payload, encodedSignature] = token.split(".");
    if (protectedHeader === undefined || payload === undefined || encodedSignature === undefined) {
      throw new Error("test signer returned a malformed compact JWT");
    }
    const signature = Buffer.from(encodedSignature, "base64url");
    signature[0] = signature[0]! ^ 1;
    const tamperedToken = `${protectedHeader}.${payload}.${signature.toString("base64url")}`;

    await expect(verifier.verify(tamperedToken)).rejects.toThrow(AccessAuthenticationError);
    await expect(verifier.verify("eyJhbGciOiJFUzI1NiJ9.e30.")).rejects.toThrow(
      AccessAuthenticationError,
    );
  });

  it.each([
    ["wrong algorithm", { alg: "HS256" as const }],
    ["wrong type", { type: "JWT" }],
    ["wrong issuer", { issuer: "https://attacker.test" }],
    ["wrong audience", { audience: "another-api" }],
    ["unknown key ID", { kid: "unknown" }],
  ])("rejects %s", async (_label, changes) => {
    const verifier = await makeVerifier();
    await expect(verifier.verify(await signToken(changes))).rejects.toThrow(
      AccessAuthenticationError,
    );
  });

  it("applies expiration clock skew but rejects tokens outside it", async () => {
    const verifier = await makeVerifier();
    await expect(
      verifier.verify(await signToken({ issuedAt: NOW_SECONDS - 603, expiresAt: NOW_SECONDS - 3 })),
    ).resolves.toBeDefined();
    await expect(
      verifier.verify(await signToken({ issuedAt: NOW_SECONDS - 606, expiresAt: NOW_SECONDS - 6 })),
    ).rejects.toThrow(AccessAuthenticationError);
  });

  it("rejects future issuance, malformed claims, and excessive lifetime", async () => {
    const verifier = await makeVerifier();
    await expect(
      verifier.verify(await signToken({ issuedAt: NOW_SECONDS + 6, expiresAt: NOW_SECONDS + 606 })),
    ).rejects.toThrow(AccessAuthenticationError);
    await expect(verifier.verify(await signToken({ payload: { sid: 123 } }))).rejects.toThrow(
      AccessAuthenticationError,
    );
    await expect(verifier.verify(await signToken({ payload: { sid: undefined } }))).rejects.toThrow(
      AccessAuthenticationError,
    );
    await expect(
      verifier.verify(await signToken({ expiresAt: NOW_SECONDS + 601 })),
    ).rejects.toThrow(AccessAuthenticationError);
  });
});

describe("remote JWKS caching", () => {
  it("uses a cached known key and refreshes once for a rotated unknown key", async () => {
    const secondPair = await generateKeyPair("ES256");
    const secondJwk: JWK = {
      ...(await exportJWK(secondPair.publicKey)),
      alg: "ES256",
      kid: "key-2",
      use: "sig",
    };
    const responses = [{ keys: [publicJwk] }, { keys: [publicJwk, secondJwk] }];
    const fetcher = vi.fn<typeof fetch>(async () =>
      Promise.resolve(
        new Response(JSON.stringify(responses.shift()), {
          headers: { "content-type": "application/json" },
          status: 200,
        }),
      ),
    );
    const resolver = createRemoteJwksKeyResolver({
      jwksUrl: "https://identity.test/.well-known/jwks.json",
      cacheTtlMs: 60_000,
      fetcher,
      now: () => NOW_SECONDS * 1_000,
    });
    const verifier = new AccessTokenVerifier({
      issuer: ISSUER,
      audience: AUDIENCE,
      algorithm: "ES256",
      accessTokenTtlSeconds: 600,
      clockSkewSeconds: 5,
      keyResolver: resolver,
      now: () => NOW_SECONDS * 1_000,
    });

    await verifier.verify(await signToken());
    await verifier.verify(await signToken());
    expect(fetcher).toHaveBeenCalledTimes(1);

    const rotatedToken = await new SignJWT({ sid: "session-2" })
      .setProtectedHeader({ alg: "ES256", kid: "key-2", typ: "at+jwt" })
      .setIssuer(ISSUER)
      .setSubject("43")
      .setAudience(AUDIENCE)
      .setIssuedAt(NOW_SECONDS)
      .setExpirationTime(NOW_SECONDS + 600)
      .setJti("token-2")
      .sign(secondPair.privateKey);
    await expect(verifier.verify(rotatedToken)).resolves.toMatchObject({ userId: "43" });
    await expect(verifier.verify(await signToken())).resolves.toMatchObject({ userId: "42" });
    expect(fetcher).toHaveBeenCalledTimes(2);
  });
});
