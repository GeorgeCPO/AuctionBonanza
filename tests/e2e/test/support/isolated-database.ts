import { randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { Client } from "pg";

const schemaPath = resolve(import.meta.dirname, "../../../../db/schema.sql");

export interface IsolatedDatabase {
  url: string;
  drop(): Promise<void>;
}

// Creates a throwaway database from the committed schema snapshot, like
// auction-service's tests (copied, since packages share no test code). An
// outbox publisher claims every publishable event in its database, so the
// end-to-end test must not share auction_db with other test files.
//
// Auction ids start at `firstAuctionId`: the Valkey cluster is shared, and
// small ids would collide with other test files' channels.
export async function createIsolatedDatabase(
  adminUrl: string,
  firstAuctionId: bigint,
): Promise<IsolatedDatabase> {
  const name = `e2e_test_${randomBytes(6).toString("hex")}`;
  const url = new URL(adminUrl);
  url.pathname = `/${name}`;

  await withClient(adminUrl, async (client) => client.query(`CREATE DATABASE ${name}`));
  // pg_dump emits psql meta-commands (\restrict) that the server cannot parse.
  const schema = (await readFile(schemaPath, "utf8"))
    .split("\n")
    .filter((line) => !line.startsWith("\\"))
    .join("\n");
  await withClient(url.toString(), async (client) => {
    await client.query(schema);
    await client.query(`SELECT setval('public.auctions_id_seq', $1, false)`, [
      firstAuctionId.toString(),
    ]);
  });

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
