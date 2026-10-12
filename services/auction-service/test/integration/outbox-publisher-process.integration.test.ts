import { execFile, spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createIsolatedDatabase, type IsolatedDatabase } from "../support/isolated-database.js";
import { eventIdOf, KafkaTestTopics } from "../support/kafka.js";
import { AuctionFixtures } from "../support/outbox-fixtures.js";
import { requireEnvironment } from "../support/environment.js";

const adminDatabaseUrl = requireEnvironment("TEST_DATABASE_URL");
const kafkaBrokers = requireEnvironment("TEST_KAFKA_BROKERS");
const serviceRoot = resolve(import.meta.dirname, "../..");
// The worker is compiled into a throwaway directory so the test never
// overwrites dist/. It lives under node_modules so that Node resolves the
// service's dependencies from it, and tooling and Git already ignore it.
const buildCacheRoot = resolve(serviceRoot, "node_modules/.cache");
let entryPoint: string;

// Runs the built worker as its own OS process, exactly as it is deployed.
describe("outbox publisher worker process", { timeout: 90_000 }, () => {
  let isolatedDatabase: IsolatedDatabase;
  let pool: Pool;
  let topics: KafkaTestTopics;
  let buildDirectory: string | undefined;

  beforeAll(async () => {
    await mkdir(buildCacheRoot, { recursive: true });
    buildDirectory = await mkdtemp(resolve(buildCacheRoot, "outbox-publisher-test-"));
    await promisify(execFile)(
      resolve(serviceRoot, "node_modules/.bin/tsc"),
      ["-p", "tsconfig.build.json", "--outDir", buildDirectory],
      { cwd: serviceRoot },
    );
    entryPoint = resolve(buildDirectory, "outbox-publisher.main.js");
    isolatedDatabase = await createIsolatedDatabase(adminDatabaseUrl);
    pool = new Pool({ connectionString: isolatedDatabase.url, max: 2 });
    topics = new KafkaTestTopics(kafkaBrokers);
    await topics.connect();
  });

  afterAll(async () => {
    await topics?.disconnect();
    await pool?.end();
    await isolatedDatabase?.drop();
    if (buildDirectory !== undefined) {
      await rm(buildDirectory, { recursive: true, force: true });
    }
  });

  it("refuses to start with invalid configuration", async () => {
    const worker = startWorker({
      DATABASE_URL: isolatedDatabase.url,
      KAFKA_BROKERS: kafkaBrokers,
      OUTBOX_PUBLISH_TIMEOUT_MS: "10000",
      OUTBOX_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS: "12000",
    });

    const [exitCode] = (await once(worker.process, "exit")) as [number | null];
    expect(exitCode).toBe(1);
    expect(worker.stderr()).toContain(
      "Invalid outbox publisher configuration: /idleInTransactionTimeoutMs: must be at least publishTimeoutMs + 5000",
    );
  });

  it("publishes pending events and exits cleanly on SIGTERM", async () => {
    const topic = await topics.create();
    const fixtures = new AuctionFixtures(pool);
    const ownerId = await fixtures.user();
    const bidderId = await fixtures.user();
    const auctionId = await fixtures.openAuction(ownerId);
    const eventIds = [
      await fixtures.acceptBid(auctionId, bidderId),
      await fixtures.acceptBid(auctionId, bidderId),
    ];

    const worker = startWorker({
      LOG_LEVEL: "debug",
      DATABASE_URL: isolatedDatabase.url,
      KAFKA_BROKERS: kafkaBrokers,
      AUCTION_EVENTS_TOPIC: topic,
      OUTBOX_PUBLISHER_CONCURRENCY: "2",
      OUTBOX_POLL_INTERVAL_MS: "20",
      OUTBOX_PUBLISH_TIMEOUT_MS: "5000",
      OUTBOX_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS: "10000",
      OUTBOX_SHUTDOWN_TIMEOUT_MS: "12000",
    });
    const exited = once(worker.process, "exit") as Promise<[number | null, string | null]>;

    const deadline = Date.now() + 30_000;
    while ((await publishedCount(eventIds)) < eventIds.length) {
      if (Date.now() > deadline) {
        throw new Error(`Worker did not publish:\n${worker.stdout()}`);
      }
      await delay(50);
    }
    worker.process.kill("SIGTERM");

    expect(await exited).toEqual([0, null]);
    const logs = worker
      .stdout()
      .split("\n")
      .filter((line) => line.startsWith("{"))
      .map((line) => JSON.parse(line) as { msg: string; eventId?: string });
    const messages = logs.map((entry) => entry.msg);
    expect(messages).toContain("outbox publisher started");
    expect(messages).toContain("shutdown requested; finishing in-flight publication cycles");
    expect(messages.at(-1)).toBe("outbox publisher stopped");
    expect(
      logs.filter((entry) => entry.msg === "outbox event published").map((entry) => entry.eventId),
    ).toEqual(eventIds);
    expect((await topics.readAll(topic)).map(eventIdOf)).toEqual(eventIds);
  });

  async function publishedCount(eventIds: string[]): Promise<number> {
    const result = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM outbox_events
         WHERE event_id = ANY($1::uuid[]) AND published_at IS NOT NULL`,
      [eventIds],
    );
    return Number(result.rows[0]!.count);
  }
});

function startWorker(environment: Record<string, string>): {
  process: ChildProcess;
  stdout: () => string;
  stderr: () => string;
} {
  const child = spawn(process.execPath, [entryPoint], {
    cwd: serviceRoot,
    // Only the explicit settings: nothing leaks in from a developer's shell.
    env: { PATH: process.env.PATH ?? "", NODE_ENV: "test", ...environment },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (chunk: Buffer) => (stdout += chunk.toString("utf8")));
  child.stderr?.on("data", (chunk: Buffer) => (stderr += chunk.toString("utf8")));
  return { process: child, stdout: () => stdout, stderr: () => stderr };
}
