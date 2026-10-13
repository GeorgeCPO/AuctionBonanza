import { Pool } from "pg";

import type { DatabaseClient, DatabasePool, DatabasePoolSettings } from "./database.types.js";

// The same thin layer over `pg` as auction-service's database.ts, copied and
// trimmed to what the settlement worker uses: every statement runs on a
// checked-out client, because every statement belongs to a transaction. The
// transaction stays explicit (`BEGIN` / `COMMIT`) in the repository.

export function createDatabasePool(settings: DatabasePoolSettings): DatabasePool {
  const pool = new Pool({
    connectionString: settings.connectionString,
    max: settings.maxConnections,
    connectionTimeoutMillis: 5_000,
    idleTimeoutMillis: 30_000,
    application_name: settings.applicationName,
    keepAlive: settings.keepAlive,
    statement_timeout: settings.statementTimeoutMs,
    lock_timeout: settings.lockTimeoutMs,
    idle_in_transaction_session_timeout: settings.idleInTransactionSessionTimeoutMs,
  });

  pool.on("error", settings.onIdleClientError);

  return wrapPool(pool);
}

export function wrapPool(pool: Pool): DatabasePool {
  return {
    connect: async () => {
      const client = await pool.connect();
      // pg-pool only listens for errors on idle clients. A checked-out
      // client's connection can still fail while no query is running, for
      // example when PostgreSQL terminates a session that is idle in a
      // transaction. Without a listener that 'error' event would crash the
      // process; instead the client's next query reports it.
      let connectionError: Error | undefined;
      const onError = (error: Error): void => {
        connectionError = error;
      };
      client.on("error", onError);
      return {
        query: async (text, values) => {
          if (connectionError !== undefined) {
            throw connectionError;
          }
          return client.query(text, values as unknown[] | undefined);
        },
        release: (error) => {
          client.removeListener("error", onError);
          client.release(error ?? connectionError);
        },
      };
    },
    end: async () => pool.end(),
  };
}

// Best-effort ROLLBACK for a failed transaction. The original error is the
// one worth reporting, and a client whose rollback failed is destroyed when
// it is released with that error.
export async function rollbackQuietly(client: DatabaseClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch {
    // Preserve the original transaction error.
  }
}

export function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
