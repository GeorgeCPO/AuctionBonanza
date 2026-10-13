import { Pool } from "pg";

// The same thin layer over `pg` as auction-service's database.ts, copied and
// trimmed to what the settlement worker uses: every statement runs on a
// checked-out client, because every statement belongs to a transaction. The
// transaction stays explicit (`BEGIN` / `COMMIT`) in the repository.

export type QueryResult = Readonly<{
  rows: Record<string, unknown>[];
}>;

export interface Database {
  connect(): Promise<DatabaseClient>;
}

export interface DatabaseClient {
  query(text: string, values?: readonly unknown[]): Promise<QueryResult>;
  // Passing an error destroys the connection instead of returning it to the
  // pool, for clients whose session or transaction state is unknown.
  release(error?: Error): void;
}

export interface DatabasePool extends Database {
  end(): Promise<void>;
}

export interface DatabasePoolSettings {
  connectionString: string;
  maxConnections: number;
  applicationName?: string;
  keepAlive?: boolean;
  // Session limits PostgreSQL enforces on the server for every connection.
  statementTimeoutMs?: number;
  lockTimeoutMs?: number;
  idleInTransactionSessionTimeoutMs?: number;
  // Errors from idle pooled connections. pg emits them as 'error' events,
  // which would crash the process without a listener.
  onIdleClientError: (error: Error) => void;
}

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
