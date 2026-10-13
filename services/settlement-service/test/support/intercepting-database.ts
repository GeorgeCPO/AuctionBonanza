import type { Database, DatabaseClient } from "../../src/shared/database.types.js";
import type { QueryInterceptor } from "./intercepting-database.types.js";

// A database that behaves like `inner` except that `intercept` may answer a
// query itself, or run it and then fail: return a result to replace the real
// one, return undefined to let the query through, or throw to fail it. This is
// how the tests produce the failures that matter at the acknowledgement
// boundary, such as a COMMIT that succeeded whose reply never arrived.
export class InterceptingDatabase implements Database {
  constructor(
    private readonly inner: Database,
    private readonly intercept: QueryInterceptor,
  ) {}

  async connect(): Promise<DatabaseClient> {
    const client = await this.inner.connect();
    return {
      query: async (text, values) =>
        (await this.intercept(text, client)) ?? client.query(text, values),
      release: (error) => client.release(error),
    };
  }
}
