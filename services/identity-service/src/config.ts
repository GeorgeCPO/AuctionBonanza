import { Type, type Static } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

const identityConfigSchema = Type.Object(
  {
    nodeEnv: Type.Union([
      Type.Literal("development"),
      Type.Literal("test"),
      Type.Literal("production"),
    ]),
    host: Type.String({ minLength: 1 }),
    port: Type.Integer({ minimum: 1, maximum: 65_535 }),
    databaseUrl: Type.String({
      pattern: "^postgres(?:ql)?://.+",
    }),
    authIssuer: Type.String({ minLength: 1, pattern: "^https?://[^\\s]+$" }),
    authAudience: Type.String({ minLength: 1, pattern: "^\\S+$" }),
    accessTokenTtlSeconds: Type.Integer({ minimum: 60, maximum: 3_600 }),
    refreshSessionTtlSeconds: Type.Integer({ minimum: 3_600, maximum: 5_184_000 }),
    accessTokenClockSkewSeconds: Type.Integer({ minimum: 0, maximum: 60 }),
    signingAlgorithm: Type.Literal("ES256"),
    activeSigningKeyId: Type.String({
      minLength: 1,
      maxLength: 128,
      pattern: "^[A-Za-z0-9._-]+$",
    }),
    signingPrivateKeyPath: Type.String({ minLength: 1 }),
    signingJwksPath: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
);

export type IdentityConfig = Readonly<Static<typeof identityConfigSchema>>;

export function parseIdentityConfig(
  environment: Readonly<Record<string, string | undefined>>,
): IdentityConfig {
  const candidate = {
    nodeEnv: environment.NODE_ENV ?? "development",
    host: environment.HOST ?? "0.0.0.0",
    port: Number(environment.PORT ?? "3001"),
    databaseUrl: environment.DATABASE_URL,
    authIssuer: environment.AUTH_ISSUER ?? "http://localhost:3001",
    authAudience: environment.AUTH_AUDIENCE ?? "auction-api",
    accessTokenTtlSeconds: Number(environment.ACCESS_TOKEN_TTL_SECONDS ?? "600"),
    refreshSessionTtlSeconds: Number(environment.REFRESH_SESSION_TTL_SECONDS ?? "2592000"),
    accessTokenClockSkewSeconds: Number(environment.ACCESS_TOKEN_CLOCK_SKEW_SECONDS ?? "5"),
    signingAlgorithm: environment.AUTH_SIGNING_ALGORITHM ?? "ES256",
    activeSigningKeyId: environment.AUTH_ACTIVE_KEY_ID,
    signingPrivateKeyPath: environment.AUTH_SIGNING_PRIVATE_KEY_PATH,
    signingJwksPath: environment.AUTH_JWKS_PATH,
  };

  if (!Value.Check(identityConfigSchema, candidate)) {
    const details = [...Value.Errors(identityConfigSchema, candidate)]
      .map((error) => `${error.path || "/"}: ${error.message}`)
      .join("; ");

    throw new Error(`Invalid identity-service configuration: ${details}`);
  }

  // TypeBox performs the runtime narrowing above; TypeScript cannot infer it
  // through Value.Check, so this assertion joins the runtime and static types.
  return candidate as IdentityConfig;
}

export function loadIdentityConfig(): IdentityConfig {
  return parseIdentityConfig(process.env);
}
