import { setTimeout as delay } from "node:timers/promises";

import { pino } from "pino";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { auctionChannel } from "../../src/bus/bus.js";
import { ValkeyBusPublisher } from "../../src/bus/valkey-bus-publisher.js";
import {
  bidAccepted,
  BusRecorder,
  captureLogger,
  KafkaTestTopics,
  ScriptedPublisher,
  startRelay,
  waitFor,
  type RelayRun,
  type StartRelayOptions,
} from "../support/relay-harness.js";
import { requireEnvironment } from "../support/environment.js";

const brokers = requireEnvironment("TEST_KAFKA_BROKERS");
const valkeyNodes = requireEnvironment("TEST_VALKEY_NODES").split(",");

describe("relay against Kafka and Valkey", { timeout: 90_000 }, () => {
  const partitions = 3;
  let topics: KafkaTestTopics;
  let valkey: ValkeyBusPublisher;
  let bus: BusRecorder;
  const runs: RelayRun[] = [];

  async function run(options: Omit<StartRelayOptions, "brokers">): Promise<RelayRun> {
    const relayRun = await startRelay({ brokers: brokers, ...options });
    runs.push(relayRun);
    return relayRun;
  }

  async function freshTopic(): Promise<{ topic: string; groupId: string }> {
    return { topic: await topics.create(partitions), groupId: topics.newGroup() };
  }

  beforeAll(async () => {
    topics = new KafkaTestTopics(brokers);
    await topics.connect();
    valkey = new ValkeyBusPublisher({
      nodes: valkeyNodes,
      commandTimeoutMs: 2_000,
      logger: pino({ level: "silent" }),
    });
    await valkey.connect();
    bus = new BusRecorder(valkeyNodes);
    await bus.connect();
  });

  afterEach(async () => {
    await Promise.all(runs.splice(0).map(async (relayRun) => relayRun.stop()));
  });

  afterAll(async () => {
    await bus?.close();
    await valkey?.close();
    await topics?.disconnect();
  });

  it("forwards events produced before a new group started, per auction in offset order, and commits them", async () => {
    const { topic, groupId } = await freshTopic();
    const auctions = ["101", "102", "103", "104"];
    await bus.subscribe(...auctions.map(auctionChannel));
    const versions = [2, 3, 4, 5, 6, 7, 8];
    // Interleaved across auctions, as a busy topic would be, and all of it
    // before the group exists: a new group starts at the oldest event.
    await topics.produce(
      topic,
      versions.flatMap((version) => auctions.map((auctionId) => bidAccepted(auctionId, version))),
    );

    const relayRun = await run({ topic, groupId, publisher: valkey });
    await waitFor(
      () => auctions.every((auctionId) => bus.versionsOf(auctionId).length === versions.length),
      "every event on its auction channel",
    );
    await relayRun.stop();

    for (const auctionId of auctions) {
      expect(bus.versionsOf(auctionId)).toEqual(versions.map(String));
    }
    expect(await topics.committedOffsetsOfNonEmptyPartitions(groupId, topic)).toEqual(
      await topics.nonEmptyEndOffsets(topic),
    );
  });

  it("resumes from its committed offset after a restart, without replaying", async () => {
    const { topic, groupId } = await freshTopic();
    const auctionId = "151";
    await bus.subscribe(auctionChannel(auctionId));
    await topics.produce(
      topic,
      [2, 3].map((version) => bidAccepted(auctionId, version)),
    );
    const first = await run({ topic, groupId, publisher: valkey });
    await waitFor(() => bus.versionsOf(auctionId).length === 2, "the first two events");
    await first.stop();

    await topics.produce(
      topic,
      [4, 5].map((version) => bidAccepted(auctionId, version)),
    );
    const second = await run({ topic, groupId, publisher: valkey });
    await waitFor(() => bus.versionsOf(auctionId).length === 4, "the next two events");
    await second.stop();

    expect(bus.versionsOf(auctionId)).toEqual(["2", "3", "4", "5"]);
  });

  it("skips records it cannot forward, reports them, and commits past them", async () => {
    const { topic, groupId } = await freshTopic();
    const auctionId = "201";
    await bus.subscribe(auctionChannel(auctionId));
    const produced = await topics.produce(topic, [
      bidAccepted(auctionId, 2),
      { ...bidAccepted(auctionId, 3), eventType: "AuctionRenamed.v9" },
      { ...bidAccepted(auctionId, 3), value: "{not json" },
      { ...bidAccepted(auctionId, 3), key: "999" },
      { ...bidAccepted(auctionId, 3), eventType: undefined },
      bidAccepted(auctionId, 3),
    ]);

    const { logger, lines } = captureLogger();
    const relayRun = await run({ topic, groupId, publisher: valkey, logger });
    await waitFor(() => bus.versionsOf(auctionId).length === 2, "both valid events");
    await relayRun.stop();

    expect(bus.versionsOf(auctionId)).toEqual(["2", "3"]);
    // The wrongly keyed record lands on another partition, so skips are
    // compared by position rather than in one global order.
    const byPosition = (entries: [number, string, string][]) =>
      entries.toSorted((a, b) => a[0] - b[0] || Number(BigInt(a[1]) - BigInt(b[1])));
    const skipped = lines
      .filter((line) => String(line.msg).startsWith("skipping"))
      .map((line) => [line.partition, line.offset, line.reason] as [number, string, string]);
    expect(byPosition(skipped)).toEqual(
      byPosition([
        [produced[1]!.partition, produced[1]!.offset.toString(), "unknown-event-type"],
        [produced[2]!.partition, produced[2]!.offset.toString(), "malformed"],
        [produced[3]!.partition, produced[3]!.offset.toString(), "malformed"],
        [produced[4]!.partition, produced[4]!.offset.toString(), "malformed"],
      ]),
    );
    expect(await topics.committedOffsetsOfNonEmptyPartitions(groupId, topic)).toEqual(
      await topics.nonEmptyEndOffsets(topic),
    );
  });

  it("retries a failed publish without committing past it or letting later events overtake it", async () => {
    const { topic, groupId } = await freshTopic();
    const auctionId = "301";
    await bus.subscribe(auctionChannel(auctionId));
    const produced = await topics.produce(
      topic,
      [2, 3, 4, 5, 6].map((version) => bidAccepted(auctionId, version)),
    );
    const { partition } = produced[0]!;

    const publisher = new ScriptedPublisher(valkey);
    let busDown = true;
    publisher.failWhile = (channel, message) =>
      busDown && channel === auctionChannel(auctionId) && message.includes('"auctionVersion":"4"');
    const relayRun = await run({ topic, groupId, publisher });

    await waitFor(() => publisher.failures.length >= 3, "repeated failures of version 4");
    // Several auto-commit intervals pass while version 4 keeps failing.
    await delay(500);
    expect(bus.versionsOf(auctionId)).toEqual(["2", "3"]);
    // Committed up to version 4's offset: 2 and 3 are done, 4 is not.
    expect((await topics.committedOffsets(groupId, topic)).get(partition)).toBe(
      produced[2]!.offset,
    );

    busDown = false;
    await waitFor(() => bus.versionsOf(auctionId).length === 5, "the rest after recovery");
    await relayRun.stop();

    expect(bus.versionsOf(auctionId)).toEqual(["2", "3", "4", "5", "6"]);
    expect(await topics.committedOffsetsOfNonEmptyPartitions(groupId, topic)).toEqual(
      await topics.nonEmptyEndOffsets(topic),
    );
  });

  // The one way this relay skips events: retention deletes records the
  // group has not read yet. The client then restarts at the oldest
  // retained record. Edges notice the version gap on that record and
  // resync; an auction with no later event stays stale. See "Offset
  // resets" in docs/REALTIME_GATEWAY.md.
  it("continues from the oldest retained event when retention deleted events it had not read", async () => {
    const { topic, groupId } = await freshTopic();
    const auctionId = "401";
    await bus.subscribe(auctionChannel(auctionId));
    await topics.produce(topic, [bidAccepted(auctionId, 2)]);
    const first = await run({ topic, groupId, publisher: valkey });
    await waitFor(() => bus.versionsOf(auctionId).length === 1, "the first event");
    await first.stop();

    const unread = await topics.produce(
      topic,
      [3, 4, 5].map((version) => bidAccepted(auctionId, version)),
    );
    const { partition } = unread[0]!;
    await topics.deleteRecordsBefore(topic, partition, unread[2]!.offset + 1n);
    await topics.produce(topic, [bidAccepted(auctionId, 6)]);

    const second = await run({ topic, groupId, publisher: valkey });
    await waitFor(() => bus.versionsOf(auctionId).length === 2, "the retained event");
    await second.stop();

    expect(bus.versionsOf(auctionId)).toEqual(["2", "6"]);
  });
});
