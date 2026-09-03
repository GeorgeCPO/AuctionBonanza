import { Pool } from "pg";

import type { IdentityConfig } from "./config.js";

export type QueryResult = Readonly<{
  rows: Record<string, unknown>[];
}>;

export interface Database {
  query(text: string, values?: readonly unknown[]): Promise<QueryResult>;
}

export function createDatabasePool(config: IdentityConfig): Pool {
  const pool = new Pool({
    connectionString: config.databaseUrl,
    max: 10,
    connectionTimeoutMillis: 5_000,
    idleTimeoutMillis: 30_000,
  });

  pool.on("error", (error) => {
    console.error("Unexpected error from an idle PostgreSQL client", error);
  });

  return pool;
}
