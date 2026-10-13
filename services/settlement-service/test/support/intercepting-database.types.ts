import type { DatabaseClient, QueryResult } from "../../src/shared/database.types.js";

// What an interceptor is given for each query on a checked-out client.
export type QueryInterceptor = (
  text: string,
  client: DatabaseClient,
) => Promise<QueryResult | undefined> | undefined;
