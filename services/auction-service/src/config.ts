import { Type, type Static } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

const auctionConfigSchema = Type.Object(
  {
    nodeEnv: Type.Union([
      Type.Literal("development"),
      Type.Literal("test"),
      Type.Literal("production"),
    ]),
    host: Type.String({ minLength: 1 }),
    port: Type.Integer({ minimum: 1, maximum: 65_535 }),
    databaseUrl: Type.String({ pattern: "^postgres(?:ql)?://.+" }),
    authIssuer: Type.String({ minLength: 1, pattern: "^https?://[^\\s]+$" }),
    authAudience: Type.String({ minLength: 1, pattern: "^\\S+$" }),
    accessTokenTtlSeconds: Type.Integer({ minimum: 60, maximum: 3_600 }),
    accessTokenClockSkewSeconds: Type.Integer({ minimum: 0, maximum: 60 }),
    authJwksUrl: Type.String({ minLength: 1, pattern: "^https?://[^\\s]+$" }),
    authJwksCacheTtlMilliseconds: Type.Integer({ minimum: 1_000, maximum: 3_600_000 }),
    supportedCurrencyCodes: Type.Array(Type.String({ pattern: "^[A-Z]{3}$" }), {
      minItems: 1,
      maxItems: 64,
    }),
  },
  { additionalProperties: false },
);

export type AuctionConfig = Readonly<Static<typeof auctionConfigSchema>>;

export function parseAuctionConfig(
  environment: Readonly<Record<string, string | undefined>>,
): AuctionConfig {
  const supportedCurrencyCodes = (environment.SUPPORTED_CURRENCY_CODES ?? "EUR,GBP,USD")
    .split(",")
    .map((code) => code.trim())
    .sort();
  const candidate = {
    nodeEnv: environment.NODE_ENV ?? "development",
    host: environment.HOST ?? "0.0.0.0",
    port: Number(environment.PORT ?? "3002"),
    databaseUrl: environment.DATABASE_URL,
    authIssuer: environment.AUTH_ISSUER ?? "http://localhost:3001",
    authAudience: environment.AUTH_AUDIENCE ?? "auction-api",
    accessTokenTtlSeconds: Number(environment.ACCESS_TOKEN_TTL_SECONDS ?? "600"),
    accessTokenClockSkewSeconds: Number(environment.ACCESS_TOKEN_CLOCK_SKEW_SECONDS ?? "5"),
    authJwksUrl: environment.AUTH_JWKS_URL ?? "http://localhost:3001/.well-known/jwks.json",
    authJwksCacheTtlMilliseconds: Number(environment.AUTH_JWKS_CACHE_TTL_MILLISECONDS ?? "300000"),
    supportedCurrencyCodes,
  };

  if (
    !Value.Check(auctionConfigSchema, candidate) ||
    new Set(supportedCurrencyCodes).size !== supportedCurrencyCodes.length
  ) {
    const details = [...Value.Errors(auctionConfigSchema, candidate)]
      .map((error) => `${error.path || "/"}: ${error.message}`)
      .join("; ");
    throw new Error(`Invalid auction-service configuration${details ? `: ${details}` : ""}`);
  }

  return candidate as AuctionConfig;
}

export function loadAuctionConfig(): AuctionConfig {
  return parseAuctionConfig(process.env);
}
