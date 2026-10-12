import { describe, expect, it } from "vitest";

import { parseIdentityConfig } from "../../src/config.js";

const validEnvironment = {
  NODE_ENV: "test",
  HOST: "127.0.0.1",
  PORT: "3101",
  DATABASE_URL: "postgresql://auction:auction_local@localhost:5432/auction_db",
  AUTH_ISSUER: "https://identity.test",
  AUTH_AUDIENCE: "auction-api",
  ACCESS_TOKEN_TTL_SECONDS: "600",
  REFRESH_SESSION_TTL_SECONDS: "2592000",
  ACCESS_TOKEN_CLOCK_SKEW_SECONDS: "5",
  AUTH_SIGNING_ALGORITHM: "ES256",
  AUTH_ACTIVE_KEY_ID: "test-key",
  AUTH_SIGNING_PRIVATE_KEY_PATH: "/keys/private.pem",
  AUTH_JWKS_PATH: "/keys/jwks.json",
};

describe("parseIdentityConfig", () => {
  it("parses a valid environment into typed runtime values", () => {
    expect(parseIdentityConfig(validEnvironment)).toEqual({
      nodeEnv: "test",
      host: "127.0.0.1",
      port: 3101,
      databaseUrl: "postgresql://auction:auction_local@localhost:5432/auction_db",
      authIssuer: "https://identity.test",
      authAudience: "auction-api",
      accessTokenTtlSeconds: 600,
      refreshSessionTtlSeconds: 2_592_000,
      accessTokenClockSkewSeconds: 5,
      signingAlgorithm: "ES256",
      activeSigningKeyId: "test-key",
      signingPrivateKeyPath: "/keys/private.pem",
      signingJwksPath: "/keys/jwks.json",
    });
  });

  it("uses local-safe defaults for non-secret server settings", () => {
    expect(
      parseIdentityConfig({
        DATABASE_URL: validEnvironment.DATABASE_URL,
        AUTH_ACTIVE_KEY_ID: "test-key",
        AUTH_SIGNING_PRIVATE_KEY_PATH: "/keys/private.pem",
        AUTH_JWKS_PATH: "/keys/jwks.json",
      }),
    ).toMatchObject({
      nodeEnv: "development",
      host: "0.0.0.0",
      port: 3001,
    });
  });

  it("rejects a missing database URL", () => {
    expect(() => parseIdentityConfig({})).toThrow("Invalid identity-service configuration");
  });

  it.each(["0", "65536", "not-a-number"])("rejects invalid port %s", (port) => {
    expect(() => parseIdentityConfig({ ...validEnvironment, PORT: port })).toThrow(
      "Invalid identity-service configuration",
    );
  });

  it("rejects unsupported environments", () => {
    expect(() => parseIdentityConfig({ ...validEnvironment, NODE_ENV: "staging" })).toThrow(
      "Invalid identity-service configuration",
    );
  });

  it("rejects non-PostgreSQL database URLs", () => {
    expect(() =>
      parseIdentityConfig({
        ...validEnvironment,
        DATABASE_URL: "mysql://localhost/auction_db",
      }),
    ).toThrow("Invalid identity-service configuration");
  });

  it.each([
    ["ACCESS_TOKEN_TTL_SECONDS", "59"],
    ["ACCESS_TOKEN_CLOCK_SKEW_SECONDS", "61"],
    ["REFRESH_SESSION_TTL_SECONDS", "3599"],
    ["AUTH_SIGNING_ALGORITHM", "HS256"],
    ["AUTH_ISSUER", "not-a-uri"],
  ])("rejects invalid authentication setting %s", (name, value) => {
    expect(() => parseIdentityConfig({ ...validEnvironment, [name]: value })).toThrow(
      "Invalid identity-service configuration",
    );
  });

  it.each(["AUTH_ACTIVE_KEY_ID", "AUTH_SIGNING_PRIVATE_KEY_PATH", "AUTH_JWKS_PATH"])(
    "requires signing configuration %s",
    (name) => {
      expect(() => parseIdentityConfig({ ...validEnvironment, [name]: undefined })).toThrow(
        "Invalid identity-service configuration",
      );
    },
  );
});
