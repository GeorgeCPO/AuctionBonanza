import { setTimeout as delay } from "node:timers/promises";

import { pino } from "pino";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { auctionChannel, CONTROL_CHANNEL } from "../src/bus/bus.js";
import { ValkeyBusPublisher } from "../src/bus/valkey-bus-publisher.js";
import {
  bidAccepted,
  BusRecorder,
  captureLogger,
  KafkaTestTopics,
  ScriptedPublisher,
  startRelay,
  waitFor,
  warmUpGroup,
  type RelayRun,
  type StartRelayOptions,
} from "./support/relay-harness.js";

const brokers = process.env.TEST_KAFKA_BROKERS;
const valkeyNodes = process.env.TEST_VALKEY_NODES?.split(",");

describe.skipIf(brokers === undefined || valkeyNodes === undefined)(
  "relay against Kafka and Valkey",
  { timeout: 90_000 },
  () => {
    const partitions = 3;
    let topics: KafkaTestTopics;
    let valkey: ValkeyBusPublisher;
    let bus: BusRecorder;
    const runs: RelayRun[] = [];

    async function run(options: Omit<StartRelayOptions, "brokers">): Promise<RelayRun> {
      const relayRun = await startRelay({ brokers: brokers!, ...options });
      runs.push(relayRun);
      return relayRun;
    }

    // A topic and a group whose relays resume from committed end offsets.
    async function warmTopic(): Promise<{ topic: string; groupId: string }> {
      const topic = await topics.create(partitions);
      const groupId = topics.newGroup();
      await warmUpGroup(topics, bus, valkey, topic, groupId, partitions);
      return { topic, groupId };
    }

    function controlMessages(): Record<string, unknown>[] {
      return bus.messages
        .filter(({ channel }) => channel === CONTROL_CHANNEL)
        .map(({ message }) => JSON.parse(message) as Record<string, unknown>);
    }

    beforeAll(async () => {
      topics = new KafkaTestTopics(brokers!);
      await topics.connect();
      valkey = new ValkeyBusPublisher({
        nodes: valkeyNodes!,
        commandTimeoutMs: 2_000,
        logger: pino({ level: "silent" }),
      });
      await valkey.connect();
      bus = new BusRecorder(valkeyNodes!);
      await bus.connect();
      await bus.subscribe(CONTROL_CHANNEL);
    });

    afterEach(async () => {
      await Promise.all(runs.splice(0).map(async (relayRun) => relayRun.stop()));
    });

    afterAll(async () => {
      await bus?.close();
      await valkey?.close();
      await topics?.disconnect();
    });

    it("forwards each auction's events to its channel in offset order and commits them", async () => {
      const { topic, groupId } = await warmTopic();
      const auctions = ["101", "102", "103", "104"];
      await bus.subscribe(...auctions.map(auctionChannel));
      const versions = [2, 3, 4, 5, 6, 7, 8];
      // Interleaved across auctions, as a busy topic would be.
      await topics.produce(
        topic,
        versions.flatMap((version) => auctions.map((auctionId) => bidAccepted(auctionId, version))),
      );

      const publisher = new ScriptedPublisher(valkey);
      const relayRun = await run({ topic, groupId, publisher });
      await waitFor(
        () => auctions.every((auctionId) => bus.versionsOf(auctionId).length === versions.length),
        "every event on its auction channel",
      );
      await relayRun.stop();

      for (const auctionId of auctions) {
        expect(bus.versionsOf(auctionId)).toEqual(versions.map(String));
      }
      // Resuming from committed offsets skips nothing, so no resync is sent.
      expect(publisher.published.some(({ channel }) => channel === CONTROL_CHANNEL)).toBe(false);
      expect(await topics.committedOffsets(groupId, topic)).toEqual(await topics.endOffsets(topic));
    });

    it("skips records it cannot forward, reports them, and commits past them", async () => {
      const { topic, groupId } = await warmTopic();
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
      expect(await topics.committedOffsets(groupId, topic)).toEqual(await topics.endOffsets(topic));
    });

    it("retries a failed publish without committing past it or letting later events overtake it", async () => {
      const { topic, groupId } = await warmTopic();
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
        busDown &&
        channel === auctionChannel(auctionId) &&
        message.includes('"auctionVersion":"4"');
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
      expect(await topics.committedOffsets(groupId, topic)).toEqual(await topics.endOffsets(topic));
    });

    it("broadcasts a resync before forwarding anything when partitions have no committed offset", async () => {
      const topic = await topics.create(partitions);
      const groupId = topics.newGroup();
      const publisher = new ScriptedPublisher(valkey);
      const controlBefore = controlMessages().length;

      await run({ topic, groupId, publisher });

      await waitFor(
        () => controlMessages().length > controlBefore,
        "a resync on the control channel",
      );
      expect(controlMessages().at(-1)).toMatchObject({ v: 1, type: "resync" });
      expect(String(controlMessages().at(-1)!.reason)).toContain("without a committed offset");
      expect(publisher.published[0]?.channel).toBe(CONTROL_CHANNEL);
    });

    it("broadcasts a resync before the next forwarded event when retention removed the committed position", async () => {
      const { topic, groupId } = await warmTopic();
      const auctionId = "401";
      await bus.subscribe(auctionChannel(auctionId));
      // Events the group never read are deleted, as retention would: its
      // committed offset now lies before the log start, so the relay's
      // position is reset to the end and those events are skipped.
      const produced = await topics.produce(
        topic,
        [2, 3, 4].map((version) => bidAccepted(auctionId, version)),
      );
      const { partition } = produced[0]!;
      await topics.deleteRecordsBefore(topic, partition, produced[2]!.offset + 1n);

      const publisher = new ScriptedPublisher(valkey);
      await run({ topic, groupId, publisher });

      // After the reset the relay sees only records produced from now on.
      let version = 5;
      const deadline = Date.now() + 30_000;
      while (bus.versionsOf(auctionId).length === 0) {
        if (Date.now() > deadline) throw new Error("No event forwarded after the reset");
        await topics.produce(topic, [bidAccepted(auctionId, version)]);
        version += 1;
        await delay(200);
      }

      const resyncIndex = publisher.published.findIndex(
        ({ channel }) => channel === CONTROL_CHANNEL,
      );
      const firstEventIndex = publisher.published.findIndex(
        ({ channel }) => channel === auctionChannel(auctionId),
      );
      expect(resyncIndex).toBeGreaterThanOrEqual(0);
      expect(resyncIndex).toBeLessThan(firstEventIndex);
      const resync = JSON.parse(publisher.published[resyncIndex]!.message) as { reason: string };
      expect(resync.reason).toContain(`partition ${partition} resumed at offset`);
      expect(bus.versionsOf(auctionId)).not.toContain("2");
    });
  },
);
