import type { Pool } from "pg";

import type { TestIssuer } from "./test-issuer.types.js";

export interface AuctionApiOptions {
  apiUrl: string;
  pool: Pool;
  issuer: TestIssuer;
}

export interface AuctionServiceOptions {
  databaseUrl: string;
  issuer: TestIssuer;
  kafkaBrokers: string;
  // The topic the outbox publisher publishes to.
  topic: string;
}
