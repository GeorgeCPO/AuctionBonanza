import { describe, expect, it } from "vitest";

import { parseIdentityConfig } from "../src/config.js";

const validEnvironment = {
  NODE_ENV: "test",
  HOST: "127.0.0.1",
  PORT: "3101",
  DATABASE_URL: "postgresql://auction:auction_local@localhost:5432/auction_db",
};

describe("parseIdentityConfig", () => {
  it("parses a valid environment into typed runtime values", () => {
    expect(parseIdentityConfig(validEnvironment)).toEqual({
      nodeEnv: "test",
      host: "127.0.0.1",
      port: 3101,
      databaseUrl: "postgresql://auction:auction_local@localhost:5432/auction_db",
    });
  });

  it("uses local-safe defaults for non-secret server settings", () => {
    expect(parseIdentityConfig({ DATABASE_URL: validEnvironment.DATABASE_URL })).toMatchObject({
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
});
