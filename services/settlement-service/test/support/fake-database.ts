import type { Database, DatabaseClient, QueryResult } from "../../src/shared/database.js";

export interface RecordedQuery {
  text: string;
  values: readonly unknown[] | undefined;
}

// A database that answers from a script and remembers what it was asked and
// how each client was handed back. Enough to test the shape of a transaction
// without PostgreSQL; the integration tests prove what the SQL does.
export class FakeDatabase implements Database {
  readonly queries: RecordedQuery[] = [];
  // How each checked-out client was released: undefined for a clean release,
  // or the error that destroyed it.
  readonly releases: (Error | undefined)[] = [];
  // Called for every query on a client. Return rows to answer; throw to fail.
  respond: (text: string) => Record<string, unknown>[] | undefined = () => undefined;

  async connect(): Promise<DatabaseClient> {
    return {
      query: async (text, values) => this.run(text, values),
      release: (error) => {
        this.releases.push(error);
      },
    };
  }

  // The statements run, with whitespace collapsed, for readable assertions.
  statements(): string[] {
    return this.queries.map(({ text }) => text.replace(/\s+/g, " ").trim());
  }

  private run(text: string, values: readonly unknown[] | undefined): QueryResult {
    this.queries.push({ text, values });
    return { rows: this.respond(text) ?? [] };
  }
}
