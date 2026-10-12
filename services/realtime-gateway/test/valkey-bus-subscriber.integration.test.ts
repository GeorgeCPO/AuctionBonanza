import { randomUUID } from "node:crypto";

import { pino } from "pino";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { auctionChannel } from "../src/bus/bus.js";
import { ValkeyBusPublisher } from "../src/bus/valkey-bus-publisher.js";
import { ValkeyBusSubscriber } from "../src/bus/valkey-bus-subscriber.js";
import { killSubscriberConnections, randomAuctionId } from "./support/edge-harness.js";
import { auctionIdsInSlot, slotOf } from "./support/hash-slots.js";
import { waitFor } from "./support/relay-harness.js";
import { TestCluster } from "./support/test-cluster.js";

const valkeyNodes = process.env.TEST_VALKEY_NODES?.split(",");

// The subscriber's one promise to the registry: once whenRestored() resolves
// after an interruption, every channel is subscribed again, so a message
// published from then on is delivered. The registry relies on it to read
// live states only when no event can fall between the read and the bus.
describe.skipIf(valkeyNodes === undefined)("ValkeyBusSubscriber", { timeout: 30_000 }, () => {
  const clientName = `subscriber-test-${randomUUID()}`;
  let subscriber: ValkeyBusSubscriber;
  let publisher: ValkeyBusPublisher;

  beforeAll(async () => {
    const logger = pino({ level: "silent" });
    subscriber = new ValkeyBusSubscriber({
      nodes: valkeyNodes!,
      clientName,
      connectTimeoutMs: 2_000,
      topologyCheckIntervalMs: 60_000,
      logger,
    });
    await subscriber.connect();
    publisher = new ValkeyBusPublisher({ nodes: valkeyNodes!, commandTimeoutMs: 2_000, logger });
    await publisher.connect();
  });

  afterAll(async () => {
    await subscriber?.close();
    await publisher?.close();
  });

  it("resolves whenRestored only once every lost connection has resubscribed", async () => {
    // Enough channels to hold a connection on more than one primary.
    const channels = Array.from({ length: 12 }, () => auctionChannel(randomAuctionId()));
    const received = new Map(channels.map((channel) => [channel, new Set<string>()]));
    for (const channel of channels) {
      await subscriber.subscribe(channel, (message) => received.get(channel)!.add(message));
    }

    // Publish the moment whenRestored() resolves, as the registry reads the
    // moment it does. Resolving before the resubscriptions are confirmed
    // would lose these messages.
    const rounds: string[] = [];
    const publishing: Promise<void>[] = [];
    subscriber.onInterruption(() => {
      const round = randomUUID();
      rounds.push(round);
      publishing.push(
        subscriber.whenRestored().then(async () => {
          await Promise.all(channels.map((channel) => publisher.publish(channel, round)));
        }),
      );
    });

    const killed = await killSubscriberConnections(valkeyNodes!, clientName);
    expect(killed).toBeGreaterThan(1);
    await waitFor(() => rounds.length === killed, "an interruption per killed connection");
    await Promise.all(publishing);

    // The kills land one by one. A round can be released while a connection
    // killed after it is not yet noticed, and lose messages on that
    // connection; its own interruption follows and covers them (the registry
    // resyncs again). The round of the last interruption is released only
    // when every connection is back, so it must reach every channel.
    const last = rounds.at(-1)!;
    await waitFor(
      () => channels.every((channel) => received.get(channel)!.has(last)),
      "the last round on every channel",
      5_000,
    );
  });
});

// node-redis follows a moved slot silently, so the subscriber compares the
// cluster topology with the owners its channels were subscribed at, and
// reports the channels of a moved slot as lost.
const topologyNodes = process.env.TEST_VALKEY_TOPOLOGY_NODES?.split(",");

describe.skipIf(topologyNodes === undefined)(
  "ValkeyBusSubscriber topology check",
  { timeout: 30_000 },
  () => {
    let cluster: TestCluster;
    let subscriber: ValkeyBusSubscriber;
    let slot: number;
    let originalOwner: Awaited<ReturnType<TestCluster["ownerOf"]>>;

    beforeEach(async () => {
      cluster = new TestCluster(topologyNodes!);
      await cluster.connect();
      subscriber = new ValkeyBusSubscriber({
        nodes: topologyNodes!,
        clientName: `topology-check-${randomUUID()}`,
        connectTimeoutMs: 2_000,
        topologyCheckIntervalMs: 100,
        logger: pino({ level: "silent" }),
      });
      await subscriber.connect();
    });

    afterEach(async () => {
      await subscriber?.close();
      await cluster?.moveSlot(slot, originalOwner);
      await cluster?.close();
    });

    it("reports the channels of a moved slot, and only those", async () => {
      const firstCandidate = Number(randomAuctionId());
      slot = slotOf(auctionChannel(String(firstCandidate)));
      const moved = auctionIdsInSlot(slot, 3, firstCandidate).map(auctionChannel);
      originalOwner = await cluster.ownerOf(slot);
      const destination = cluster.primaries.find((primary) => primary.id !== originalOwner.id)!;
      // A channel whose slot stays where it is.
      let unmoved = auctionChannel(randomAuctionId());
      while (slotOf(unmoved) === slot) {
        unmoved = auctionChannel(randomAuctionId());
      }

      const lost: string[] = [];
      subscriber.onSubscriptionLost((channel) => lost.push(channel));
      for (const channel of [...moved, unmoved]) {
        await subscriber.subscribe(channel, () => {});
      }

      await cluster.moveSlot(slot, destination);

      await waitFor(() => lost.length >= moved.length, "the moved channels to be reported", 5_000);
      // One check reports everything it found in one synchronous loop, so
      // anything it wrongly reported would already be in `lost`.
      expect(lost.toSorted()).toEqual(moved.toSorted());
    });
  },
);
