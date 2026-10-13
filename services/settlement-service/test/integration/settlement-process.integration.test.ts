import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { SETTLEMENT_LOG_MESSAGES } from "../../src/settlement.log-messages.js";
import { AuctionFixtures } from "../support/auction-fixtures.js";
import { requireEnvironment } from "../support/environment.js";
import { createIsolatedDatabase, type IsolatedDatabase } from "../support/isolated-database.js";
import { KafkaTestTopics, outboxRecord, waitFor } from "../support/kafka-harness.js";
import { compileService, startServiceProcess } from "../support/service-process.js";
import type { CompiledService } from "../support/service-process.types.js";

const brokers = requireEnvironment("TEST_KAFKA_BROKERS");
const adminDatabaseUrl = requireEnvironment("TEST_DATABASE_URL");

// Runs the built worker as its own OS process, exactly as it is deployed.
describe("settlement process", { timeout: 180_000 }, () => {
  let service: CompiledService;
  let isolated: IsolatedDatabase;
  let pool: Pool;
  let fixtures: AuctionFixtures;
  let topics: KafkaTestTopics;

  beforeAll(async () => {
    topics = new KafkaTestTopics(brokers);
    // Independent of each other; the compile is the long pole.
    [service, isolated] = await Promise.all([
      compileService("settlement-test"),
      createIsolatedDatabase(adminDatabaseUrl),
      topics.connect(),
    ]);
    pool = new Pool({ connectionString: isolated.url, max: 4 });
    fixtures = new AuctionFixtures(pool);
  });

  afterAll(async () => {
    await Promise.all([
      topics?.disconnect(),
      service?.remove(),
      (async () => {
        await pool?.end();
        await isolated?.drop();
      })(),
    ]);
  });

  function environment(
    topic: string,
    deadLetterTopic: string,
    groupId: string,
  ): Record<string, string> {
    return {
      LOG_LEVEL: "debug",
      DATABASE_URL: isolated.url,
      KAFKA_BROKERS: brokers,
      KAFKA_GROUP_ID: groupId,
      AUCTION_EVENTS_TOPIC: topic,
      SETTLEMENT_DEAD_LETTER_TOPIC: deadLetterTopic,
      // The broker's minimum, so a killed worker's partitions move quickly.
      SETTLEMENT_SESSION_TIMEOUT_MS: "6000",
    };
  }

  it("refuses to start with invalid configuration", async () => {
    const worker = startServiceProcess(service, "settlement.main.js", { KAFKA_BROKERS: brokers });

    const [exitCode] = await worker.exited;

    expect(exitCode).toBe(1);
    expect(worker.stderr()).toContain("Invalid settlement configuration: /databaseUrl");
  });

  it("creates no second order after a crash between the database commit and the offset commit, then drains cleanly on SIGTERM", async () => {
    const topic = await topics.create(3);
    const deadLetterTopic = await topics.create(1);
    const groupId = topics.newGroup();
    const ownerId = await fixtures.user();
    const bidderId = await fixtures.user();
    const first = await fixtures.soldAuction(ownerId, bidderId, "1500");
    const second = await fixtures.soldAuction(ownerId, bidderId, "1600");
    await topics.produce(topic, [outboxRecord(first.event), outboxRecord(second.event)]);

    // The first offset commit is a minute away, so the worker is killed after
    // its database commits and before Kafka hears about any of them.
    const crashing = startServiceProcess(service, "settlement.main.js", {
      ...environment(topic, deadLetterTopic, groupId),
      SETTLEMENT_COMMIT_INTERVAL_MS: "60000",
    });
    await waitFor(async () => (await fixtures.orders()).length === 2, "both orders", 60_000);
    const ordersBeforeCrash = await fixtures.orders();
    crashing.process.kill("SIGKILL");
    expect(await crashing.exited).toEqual([null, "SIGKILL"]);
    const beforeRestart = await topics.commitProgress(groupId, topic);
    expect([...beforeRestart.committed.values()].filter((offset) => offset !== undefined)).toEqual(
      [],
    );

    // The next worker resumes from the last committed offset, which is the
    // beginning: both events are delivered again, and both are recognised.
    const restarted = startServiceProcess(
      service,
      "settlement.main.js",
      environment(topic, deadLetterTopic, groupId),
    );
    const repeats = () =>
      restarted.logs().filter((line) => line.msg === SETTLEMENT_LOG_MESSAGES.duplicate);
    await waitFor(
      () => repeats().length === 2,
      () => `the redelivery:\n${restarted.stdout()}\n${restarted.stderr()}`,
      90_000,
    );

    expect(await restarted.stop()).toEqual([0, null]);
    // Delivered twice, settled once.
    expect(await fixtures.orders()).toEqual(ordersBeforeCrash);
    expect(await fixtures.processedEventIds()).toHaveLength(2);
    const messages = restarted.logs().map((line) => line.msg);
    expect(messages).toContain(SETTLEMENT_LOG_MESSAGES.started);
    expect(messages).toContain(SETTLEMENT_LOG_MESSAGES.shutdownRequested);
    expect(messages.at(-1)).toBe(SETTLEMENT_LOG_MESSAGES.stopped);
    // A clean shutdown commits what it handled.
    const afterStop = await topics.commitProgress(groupId, topic);
    expect(afterStop.committed).toEqual(afterStop.ends);
    expect(await topics.readAll(deadLetterTopic)).toEqual([]);
  });
});
