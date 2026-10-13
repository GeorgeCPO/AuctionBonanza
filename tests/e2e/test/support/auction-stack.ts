import { randomUUID } from "node:crypto";

import type { AuctionApiOptions, AuctionServiceOptions } from "./auction-stack.types.js";
import { freePort, startServiceProcess } from "./services.js";
import type { CompiledService, ServiceProcess } from "./services.types.js";

// The processes one test started, so afterAll can stop them all.
export class ProcessGroup {
  private readonly processes: ServiceProcess[] = [];

  start(
    service: CompiledService,
    entryFile: string,
    environment: Record<string, string>,
  ): ServiceProcess {
    const child = startServiceProcess(service, entryFile, environment);
    this.processes.push(child);
    return child;
  }

  async stopAll(): Promise<void> {
    await Promise.all(this.processes.map(async (child) => child.stop()));
  }
}

// Starts the auction-service API, its outbox publisher and its finalizer, each
// as its own process, and returns the API's base URL.
export async function startAuctionService(
  processes: ProcessGroup,
  service: CompiledService,
  options: AuctionServiceOptions,
): Promise<string> {
  const api = processes.start(service, "index.js", {
    HOST: "127.0.0.1",
    PORT: String(await freePort()),
    DATABASE_URL: options.databaseUrl,
    AUTH_ISSUER: options.issuer.issuer,
    AUTH_AUDIENCE: options.issuer.audience,
    AUTH_JWKS_URL: options.issuer.jwksUrl,
  });
  const apiUrl = /http:\/\/\S+/.exec(String((await api.waitForLog("Server listening at")).msg))![0];
  processes.start(service, "outbox-publisher.main.js", {
    DATABASE_URL: options.databaseUrl,
    KAFKA_BROKERS: options.kafkaBrokers,
    AUCTION_EVENTS_TOPIC: options.topic,
    OUTBOX_POLL_INTERVAL_MS: "100",
  });
  processes.start(service, "finalizer.main.js", {
    DATABASE_URL: options.databaseUrl,
    FINALIZER_POLL_INTERVAL_MS: "100",
  });
  return apiUrl;
}

// What the end-to-end tests do to a running auction-service: create users in
// the database (identity is stood in for), call the API as one of them, and
// move an auction's deadline into the past.
export class AuctionApi {
  constructor(private readonly options: AuctionApiOptions) {}

  async insertUser(): Promise<string> {
    const result = await this.options.pool.query<{ id: string }>(
      `INSERT INTO users (email, username, password_hash)
       VALUES ($1, 'End to end', '$argon2id$test')
       RETURNING id::text`,
      [`e2e-${randomUUID()}@example.com`],
    );
    return result.rows[0]!.id;
  }

  async post(path: string, userId: string, body: unknown): Promise<Response> {
    return fetch(`${this.options.apiUrl}${path}`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${this.options.issuer.accessToken(userId)}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    });
  }

  async createAuction(ownerId: string, title: string, description: string): Promise<string> {
    const created = await this.post("/v1/auctions", ownerId, {
      title,
      description,
      currencyCode: "USD",
      endsAt: new Date(Date.now() + 3_600_000).toISOString(),
      startingPriceMinor: "1000",
      minimumBidIncrementMinor: "100",
    });
    if (created.status !== 201) {
      throw new Error(`creating an auction answered ${created.status}: ${await created.text()}`);
    }
    return ((await created.json()) as { id: string }).id;
  }

  // Places a bid and returns its id.
  async placeBid(auctionId: string, bidderId: string, amountMinor: string): Promise<string> {
    const placed = await this.post(`/v1/auctions/${auctionId}/bids`, bidderId, {
      amountMinor,
      idempotencyKey: randomUUID(),
    });
    if (placed.status !== 201) {
      throw new Error(`placing a bid answered ${placed.status}: ${await placed.text()}`);
    }
    return ((await placed.json()) as { bid: { id: string } }).bid.id;
  }

  // Time cannot be waited for (an auction lasts at least a minute), so the
  // deadline is moved into the past. Everything after that is the finalizer
  // and the services downstream of it acting on their own.
  async passDeadline(auctionId: string): Promise<void> {
    await this.options.pool.query(
      `UPDATE auctions
       SET starts_at = clock_timestamp() - interval '2 hours',
           ends_at = clock_timestamp() - interval '1 second'
       WHERE id = $1`,
      [auctionId],
    );
  }
}
