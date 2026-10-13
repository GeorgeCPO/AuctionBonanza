import { randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { Client } from "pg";

const schemaPath = resolve(import.meta.dirname, "../../../../db/schema.sql");

export interface IsolatedDatabase {
  url: string;
  drop(): Promise<void>;
}

// Creates a throwaway database from the committed schema snapshot, so a test
// file owns every row it reads back and cannot be disturbed by another file
// sharing auction_db. Copied from auction-service's test support: tests keep
// no code in common.
export async function createIsolatedDatabase(adminUrl: string): Promise<IsolatedDatabase> {
  const name = `settlement_test_${randomBytes(6).toString("hex")}`;
  const url = new URL(adminUrl);
  url.pathname = `/${name}`;

  await withClient(adminUrl, async (client) => client.query(`CREATE DATABASE ${name}`));
  // pg_dump emits psql meta-commands (\restrict) that the server cannot parse.
  const schema = (await readFile(schemaPath, "utf8"))
    .split("\n")
    .filter((line) => !line.startsWith("\\"))
    .join("\n");
  await withClient(url.toString(), async (client) => client.query(schema));

  return {
    url: url.toString(),
    drop: async () => {
      await withClient(adminUrl, async (client) =>
        client.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`),
      );
    },
  };
}

async function withClient(
  url: string,
  action: (client: Client) => Promise<unknown>,
): Promise<void> {
  const client = new Client({ connectionString: url });
  await client.connect();
  try {
    await action(client);
  } finally {
    await client.end();
  }
}
