import { describe, expect, it } from "vitest";

import { createDatabasePool } from "../src/database.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(databaseUrl === undefined)("createDatabasePool", () => {
  it("applies the requested session settings to every connection", async () => {
    const pool = createDatabasePool({
      connectionString: databaseUrl!,
      maxConnections: 1,
      applicationName: "database-pool-settings-test",
      keepAlive: true,
      statementTimeoutMs: 10_000,
      lockTimeoutMs: 5_000,
      idleInTransactionSessionTimeoutMs: 30_000,
      onIdleClientError: () => {},
    });

    try {
      const client = await pool.connect();
      try {
        const result = await client.query(
          `SELECT current_setting('application_name') AS application_name,
                  current_setting('statement_timeout') AS statement_timeout,
                  current_setting('lock_timeout') AS lock_timeout,
                  current_setting('idle_in_transaction_session_timeout')
                    AS idle_in_transaction_session_timeout`,
        );
        expect(result.rows[0]).toEqual({
          application_name: "database-pool-settings-test",
          statement_timeout: "10s",
          lock_timeout: "5s",
          idle_in_transaction_session_timeout: "30s",
        });
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  });
});
