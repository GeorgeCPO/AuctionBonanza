import { afterEach, describe, expect, it, vi } from "vitest";

import type { IdentityConfig } from "../src/config.js";
import { createDatabasePool } from "../src/database.js";

const config: IdentityConfig = {
  nodeEnv: "test",
  host: "127.0.0.1",
  port: 3001,
  databaseUrl: "postgresql://test:test@localhost:5432/auction_db",
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe("createDatabasePool", () => {
  it("handles errors emitted by idle PostgreSQL clients", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const pool = createDatabasePool(config);
    const error = new Error("database restarted");

    expect(() => pool.emit("error", error)).not.toThrow();
    expect(consoleError).toHaveBeenCalledWith(
      "Unexpected error from an idle PostgreSQL client",
      error,
    );

    await pool.end();
  });
});
