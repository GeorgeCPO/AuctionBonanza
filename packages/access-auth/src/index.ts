import {
  importJWK,
  jwtVerify,
  type JSONWebKeySet,
  type JWK,
  type JWSHeaderParameters,
  type JWTVerifyGetKey,
} from "jose";

const ACCESS_TOKEN_TYPE = "at+jwt";
const MAX_ACCESS_TOKEN_BYTES = 16_384;

export interface AuthenticatedPrincipal {
  userId: string;
  sessionId: string;
  tokenId: string;
}

export class AccessAuthenticationError extends Error {
  constructor() {
    super("Invalid access token");
    this.name = "AccessAuthenticationError";
  }
}

export function parseBearerAuthorization(authorization: string | string[] | undefined): string {
  if (typeof authorization !== "string") {
    throw new AccessAuthenticationError();
  }

  const match = /^Bearer ([^\s,]+)$/i.exec(authorization);
  if (match?.[1] === undefined) {
    throw new AccessAuthenticationError();
  }

  return match[1];
}

export interface AccessTokenVerifierOptions {
  issuer: string;
  audience: string;
  algorithm: "ES256";
  accessTokenTtlSeconds: number;
  clockSkewSeconds: number;
  keyResolver: JWTVerifyGetKey;
  now?: () => number;
}

export class AccessTokenVerifier {
  readonly #options: AccessTokenVerifierOptions;

  constructor(options: AccessTokenVerifierOptions) {
    this.#options = options;
  }

  async verify(token: string): Promise<AuthenticatedPrincipal> {
    if (token.length === 0 || Buffer.byteLength(token, "utf8") > MAX_ACCESS_TOKEN_BYTES) {
      throw new AccessAuthenticationError();
    }

    try {
      const nowMilliseconds = this.#options.now?.() ?? Date.now();
      const { payload, protectedHeader } = await jwtVerify(token, this.#options.keyResolver, {
        algorithms: [this.#options.algorithm],
        audience: this.#options.audience,
        clockTolerance: this.#options.clockSkewSeconds,
        currentDate: new Date(nowMilliseconds),
        issuer: this.#options.issuer,
        requiredClaims: ["iat", "exp", "jti", "sub", "sid"],
        typ: ACCESS_TOKEN_TYPE,
      });

      if (
        protectedHeader.alg !== this.#options.algorithm ||
        protectedHeader.typ !== ACCESS_TOKEN_TYPE ||
        typeof protectedHeader.kid !== "string" ||
        protectedHeader.kid.length === 0 ||
        typeof payload.sub !== "string" ||
        payload.sub.length === 0 ||
        typeof payload.sid !== "string" ||
        payload.sid.length === 0 ||
        typeof payload.jti !== "string" ||
        payload.jti.length === 0 ||
        payload.iss !== this.#options.issuer ||
        payload.aud !== this.#options.audience ||
        !Number.isSafeInteger(payload.iat) ||
        !Number.isSafeInteger(payload.exp)
      ) {
        throw new AccessAuthenticationError();
      }

      const issuedAt = payload.iat as number;
      const expiresAt = payload.exp as number;
      const now = Math.floor(nowMilliseconds / 1_000);

      if (
        expiresAt <= issuedAt ||
        expiresAt - issuedAt > this.#options.accessTokenTtlSeconds ||
        issuedAt > now + this.#options.clockSkewSeconds
      ) {
        throw new AccessAuthenticationError();
      }

      return {
        userId: payload.sub,
        sessionId: payload.sid,
        tokenId: payload.jti,
      };
    } catch (error) {
      if (error instanceof AccessAuthenticationError) {
        throw error;
      }

      throw new AccessAuthenticationError();
    }
  }
}

type ImportedJwk = Awaited<ReturnType<typeof importJWK>>;

function assertPublicEs256Jwk(value: unknown): asserts value is JWK & { kid: string } {
  if (
    typeof value !== "object" ||
    value === null ||
    !("kid" in value) ||
    typeof value.kid !== "string" ||
    value.kid.length === 0 ||
    !("kty" in value) ||
    value.kty !== "EC" ||
    !("crv" in value) ||
    value.crv !== "P-256" ||
    !("alg" in value) ||
    value.alg !== "ES256" ||
    !("use" in value) ||
    value.use !== "sig" ||
    "d" in value
  ) {
    throw new Error("JWKS contains an invalid ES256 public signing key");
  }
}

async function importPublicKeys(
  jwks: JSONWebKeySet,
  maximumKeys: number,
): Promise<Map<string, ImportedJwk>> {
  if (!Array.isArray(jwks.keys) || jwks.keys.length === 0 || jwks.keys.length > maximumKeys) {
    throw new Error("JWKS key count is outside the configured bounds");
  }

  const imported = new Map<string, ImportedJwk>();
  for (const jwk of jwks.keys) {
    assertPublicEs256Jwk(jwk);
    if (imported.has(jwk.kid)) {
      throw new Error("JWKS contains duplicate key IDs");
    }
    imported.set(jwk.kid, await importJWK(jwk, "ES256"));
  }
  return imported;
}

function keyIdFromHeader(header: JWSHeaderParameters): string {
  if (
    header.alg !== "ES256" ||
    header.typ !== ACCESS_TOKEN_TYPE ||
    typeof header.kid !== "string" ||
    header.kid.length === 0
  ) {
    throw new AccessAuthenticationError();
  }
  return header.kid;
}

export async function createStaticJwksKeyResolver(
  jwks: JSONWebKeySet,
  maximumKeys = 8,
): Promise<JWTVerifyGetKey> {
  const keys = await importPublicKeys(jwks, maximumKeys);

  return async (header) => {
    const key = keys.get(keyIdFromHeader(header));
    if (key === undefined) {
      throw new AccessAuthenticationError();
    }
    return key;
  };
}

export interface RemoteJwksKeyResolverOptions {
  jwksUrl: string;
  cacheTtlMs: number;
  maximumKeys?: number;
  fetcher?: typeof fetch;
  now?: () => number;
}

export function createRemoteJwksKeyResolver(
  options: RemoteJwksKeyResolverOptions,
): JWTVerifyGetKey {
  const maximumKeys = options.maximumKeys ?? 8;
  const fetcher = options.fetcher ?? fetch;
  let keys = new Map<string, ImportedJwk>();
  let expiresAt = 0;
  let refreshInFlight: Promise<void> | undefined;

  const refresh = async (): Promise<void> => {
    if (refreshInFlight !== undefined) {
      return refreshInFlight;
    }

    refreshInFlight = (async () => {
      const response = await fetcher(options.jwksUrl, {
        headers: { accept: "application/json" },
        signal: AbortSignal.timeout(5_000),
      });
      if (!response.ok) {
        throw new Error(`JWKS endpoint returned HTTP ${response.status}`);
      }

      const candidate = (await response.json()) as JSONWebKeySet;
      keys = await importPublicKeys(candidate, maximumKeys);
      expiresAt = (options.now?.() ?? Date.now()) + options.cacheTtlMs;
    })();

    try {
      await refreshInFlight;
    } finally {
      refreshInFlight = undefined;
    }
  };

  return async (header) => {
    const keyId = keyIdFromHeader(header);
    const now = options.now?.() ?? Date.now();
    let key = keys.get(keyId);

    if (key !== undefined && now < expiresAt) {
      return key;
    }

    await refresh();
    key = keys.get(keyId);
    if (key === undefined) {
      throw new AccessAuthenticationError();
    }
    return key;
  };
}
