import { describe, expect, it } from "vitest";

import { parseAuctionConfig } from "../../src/config.js";

const validEnvironment = {
  NODE_ENV: "test",
  HOST: "127.0.0.1",
  PORT: "3102",
  DATABASE_URL: "postgresql://auction:auction_local@localhost:5432/auction_db",
  AUTH_ISSUER: "https://identity.test",
  AUTH_AUDIENCE: "auction-api",
  ACCESS_TOKEN_TTL_SECONDS: "600",
  ACCESS_TOKEN_CLOCK_SKEW_SECONDS: "5",
  AUTH_JWKS_URL: "https://identity.test/.well-known/jwks.json",
  AUTH_JWKS_CACHE_TTL_MILLISECONDS: "300000",
  SUPPORTED_CURRENCY_CODES: "USD,EUR,GBP",
};

describe("parseAuctionConfig", () => {
  it("parses, trims, and sorts valid runtime configuration", () => {
    expect(parseAuctionConfig(validEnvironment)).toEqual({
      nodeEnv: "test",
      host: "127.0.0.1",
      port: 3102,
      databaseUrl: validEnvironment.DATABASE_URL,
      authIssuer: "https://identity.test",
      authAudience: "auction-api",
      accessTokenTtlSeconds: 600,
      accessTokenClockSkewSeconds: 5,
      authJwksUrl: "https://identity.test/.well-known/jwks.json",
      authJwksCacheTtlMilliseconds: 300_000,
      supportedCurrencyCodes: ["EUR", "GBP", "USD"],
    });
  });

  it("uses local-safe defaults for non-secret settings", () => {
    expect(parseAuctionConfig({ DATABASE_URL: validEnvironment.DATABASE_URL })).toMatchObject({
      nodeEnv: "development",
      host: "0.0.0.0",
      port: 3002,
      supportedCurrencyCodes: ["EUR", "GBP", "USD"],
    });
  });

  it("rejects a missing database URL", () => {
    expect(() => parseAuctionConfig({})).toThrow("Invalid auction-service configuration");
  });

  it.each(["usd", "US", "US1", "USD,USD", ""])(
    "rejects invalid supported currency configuration %s",
    (codes) => {
      expect(() =>
        parseAuctionConfig({ ...validEnvironment, SUPPORTED_CURRENCY_CODES: codes }),
      ).toThrow("Invalid auction-service configuration");
    },
  );

  it.each([
    ["PORT", "0"],
    ["ACCESS_TOKEN_TTL_SECONDS", "59"],
    ["ACCESS_TOKEN_CLOCK_SKEW_SECONDS", "61"],
    ["AUTH_JWKS_CACHE_TTL_MILLISECONDS", "999"],
    ["AUTH_JWKS_URL", "not-a-url"],
  ])("rejects invalid setting %s", (name, value) => {
    expect(() => parseAuctionConfig({ ...validEnvironment, [name]: value })).toThrow(
      "Invalid auction-service configuration",
    );
  });
});
