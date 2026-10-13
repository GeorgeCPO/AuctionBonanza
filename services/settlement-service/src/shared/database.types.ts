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
