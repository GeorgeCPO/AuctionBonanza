import { Pool } from "pg";

import type { AuctionConfig } from "./config.js";

export type QueryResult = Readonly<{
  rows: Record<string, unknown>[];
  rowCount?: number | null;
}>;

export interface Database {
  query(text: string, values?: readonly unknown[]): Promise<QueryResult>;
  connect(): Promise<DatabaseClient>;
}

export interface DatabaseClient {
  query(text: string, values?: readonly unknown[]): Promise<QueryResult>;
  release(): void;
}

export interface DatabasePool extends Database {
  end(): Promise<void>;
}

export function createDatabasePool(config: AuctionConfig): DatabasePool {
  const pool = new Pool({
    connectionString: config.databaseUrl,
    max: 10,
    connectionTimeoutMillis: 5_000,
    idleTimeoutMillis: 30_000,
  });

  pool.on("error", (error) => {
    console.error("Unexpected error from an idle PostgreSQL client", error);
  });

  return {
    query: async (text, values) => pool.query(text, values as unknown[] | undefined),
    connect: async () => {
      const client = await pool.connect();
      return {
        query: async (text, values) => client.query(text, values as unknown[] | undefined),
        release: () => client.release(),
      };
    },
    end: async () => pool.end(),
  };
}
