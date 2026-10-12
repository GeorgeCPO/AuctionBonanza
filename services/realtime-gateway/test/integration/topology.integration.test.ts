import { randomUUID } from "node:crypto";

import { pino } from "pino";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { auctionChannel, serializeAuctionEvent } from "../../src/bus/bus.js";
import { ValkeyBusSubscriber } from "../../src/bus/valkey-bus-subscriber.js";
import {
  bidAcceptedEvent,
  liveState,
  LiveStateStub,
  randomAuctionId,
  SseTestClient,
  startEdge,
  TestBus,
} from "../support/edge-harness.js";
import type { EdgeRun } from "../support/edge-harness.types.js";
import { auctionIdsInSlot, slotOf } from "../support/hash-slots.js";
import { waitFor } from "../support/relay-harness.js";
import { TestCluster } from "../support/test-cluster.js";
import type { Primary } from "../support/test-cluster.types.js";
import { requireEnvironment } from "../support/environment.js";

// The topology cluster, which only tests that change the topology use. Moving
// a slot on the cluster the other tests share could break a test that happens
// to use a channel in that slot. Every test that changes it is in this file,
// because vitest runs files in parallel and two changes at once would
// interfere.
const topologyNodes = requireEnvironment("TEST_VALKEY_TOPOLOGY_NODES").split(",");

// Step 2 exercise: the cluster moves a hash slot to another primary while an
// edge watches auctions in it, as it does when it is resharded.
//
// The primary that loses the slot tells its subscribers with `sunsubscribe`,
// and node-redis subscribes again at the new owner. That takes a few
// milliseconds, and nothing reports the messages published in between. The
// edge's guarantee is that a client ends up with the current state without
// anything else happening, so these tests publish exactly in that window.
describe("a slot moving to another primary", { timeout: 60_000 }, () => {
  const auctionsInSlot = 5;
  // The window is a few milliseconds wide and the test process publishes
  // right after the move, so it is missed only if the machine stalls the
  // test between two steps. Each miss moves the slot again.
  const maxAttemptsToLoseAnEvent = 10;
  let stub: LiveStateStub;
  let bus: TestBus;
  let cluster: TestCluster;
  let edge: EdgeRun;
  let client: SseTestClient | undefined;
  let slot: number;
  let originalOwner: Primary;
  let otherPrimary: Primary;
  let currentOwner: Primary;
  let auctionIds: string[];

  beforeEach(async () => {
    stub = new LiveStateStub();
    await stub.start();
    bus = new TestBus(topologyNodes);
    await bus.connect();
    cluster = new TestCluster(topologyNodes);
    await cluster.connect();

    // Several auctions whose channels share one slot, so one move affects
    // them all.
    const firstCandidate = Number(randomAuctionId());
    slot = slotOf(auctionChannel(String(firstCandidate)));
    auctionIds = auctionIdsInSlot(slot, auctionsInSlot, firstCandidate);
    originalOwner = await cluster.ownerOf(slot);
    currentOwner = originalOwner;
    otherPrimary = cluster.primaries.find((primary) => primary.id !== originalOwner.id)!;

    for (const auctionId of auctionIds) {
      stub.states.set(auctionId, liveState(auctionId, 1));
    }
    edge = await startEdge({
      valkeyNodes: topologyNodes,
      auctionServiceUrl: stub.url,
      topologyCheckIntervalMs: 200,
    });
    client = await SseTestClient.open(`${edge.url}/v1/stream?auctions=${auctionIds.join(",")}`);
    for (const auctionId of auctionIds) {
      await client.waitForTimeline(auctionId, ["snapshot@1"]);
    }
  });

  afterEach(async () => {
    client?.close();
    await edge?.stop();
    // Leave the topology as it was found.
    await cluster?.moveSlot(slot, originalOwner);
    await cluster?.close();
    await bus?.close();
    await stub?.stop();
  });

  // Commits `version` for every auction, moves the slot, and publishes each
  // event to the new owner the moment the move is done. Repeats with the
  // next version until at least one event reached no subscriber, which means
  // the edge really missed it. Returns the version that was published.
  async function loseAnEventDuringAMove(firstVersion: number): Promise<number> {
    for (let attempt = 0; attempt < maxAttemptsToLoseAnEvent; attempt += 1) {
      const version = firstVersion + attempt;
      for (const auctionId of auctionIds) {
        stub.states.set(auctionId, liveState(auctionId, version));
      }
      const destination = currentOwner.id === originalOwner.id ? otherPrimary : originalOwner;
      await cluster.moveSlot(slot, destination);
      currentOwner = destination;

      const delivered: number[] = [];
      for (const auctionId of auctionIds) {
        const message = serializeAuctionEvent(
          "BidAccepted.v1",
          bidAcceptedEvent(auctionId, version),
        );
        delivered.push(await cluster.publishTo(destination, auctionChannel(auctionId), message));
      }
      if (delivered.some((count) => count === 0)) {
        return version;
      }
      // The edge had subscribed again before the publish, so nothing was
      // lost. Let every client reach this version and move the slot again.
      await expect
        .poll(latestVersions, { timeout: 5_000, interval: 50 })
        .toEqual(auctionIds.map(() => String(version)));
    }
    throw new Error(
      `The edge subscribed again before the event was published in ${maxAttemptsToLoseAnEvent} moves in a row`,
    );
  }

  function latestVersions(): (string | undefined)[] {
    return auctionIds.map((auctionId) => {
      const last = client!.timeline(auctionId).at(-1);
      return last?.split("@")[1];
    });
  }

  async function subscribersAtTheOwner(): Promise<number[]> {
    return Promise.all(
      auctionIds.map((auctionId) => cluster.subscribersOf(currentOwner, auctionChannel(auctionId))),
    );
  }

  it("leaves every client at the current version without another event", async () => {
    const lostVersion = await loseAnEventDuringAMove(2);

    // Whether the edge heard the event, or noticed it missed it and read
    // the state again, every client must end up at that version.
    await expect
      .poll(latestVersions, { timeout: 5_000, interval: 50 })
      .toEqual(auctionIds.map(() => String(lostVersion)));
  });

  it("repairs a missed event once the next event of the auction arrives", async () => {
    const lostVersion = await loseAnEventDuringAMove(2);
    // Wait until the edge is subscribed at the new owner again, so that the
    // next event is delivered.
    await expect
      .poll(subscribersAtTheOwner, { timeout: 5_000, interval: 50 })
      .toEqual(auctionIds.map(() => 1));

    const nextVersion = lostVersion + 1;
    for (const auctionId of auctionIds) {
      stub.states.set(auctionId, liveState(auctionId, nextVersion));
      await bus.publishBid(auctionId, nextVersion);
    }

    await expect
      .poll(latestVersions, { timeout: 5_000, interval: 50 })
      .toEqual(auctionIds.map(() => String(nextVersion)));
  });
});

// node-redis follows a moved slot silently, so the subscriber compares the
// cluster topology with the owners its channels were subscribed at, and
// reports the channels of a moved slot as lost.
describe("ValkeyBusSubscriber topology check", { timeout: 30_000 }, () => {
  let cluster: TestCluster;
  let subscriber: ValkeyBusSubscriber;
  let slot: number | undefined;
  let originalOwner: Primary | undefined;

  beforeEach(async () => {
    slot = undefined;
    originalOwner = undefined;
    cluster = new TestCluster(topologyNodes);
    await cluster.connect();
    subscriber = new ValkeyBusSubscriber({
      nodes: topologyNodes,
      clientName: `topology-check-${randomUUID()}`,
      connectTimeoutMs: 2_000,
      topologyCheckIntervalMs: 100,
      logger: pino({ level: "silent" }),
    });
    await subscriber.connect();
  });

  afterEach(async () => {
    await subscriber?.close();
    // Only if the test got as far as moving the slot.
    if (slot !== undefined && originalOwner !== undefined) {
      await cluster?.moveSlot(slot, originalOwner);
    }
    await cluster?.close();
  });

  it("reports the channels of a moved slot, and only those", async () => {
    const firstCandidate = Number(randomAuctionId());
    const movedSlot = slotOf(auctionChannel(String(firstCandidate)));
    const moved = auctionIdsInSlot(movedSlot, 3, firstCandidate).map(auctionChannel);
    const owner = await cluster.ownerOf(movedSlot);
    const destination = cluster.primaries.find((primary) => primary.id !== owner.id)!;
    slot = movedSlot;
    originalOwner = owner;
    // A channel whose slot stays where it is.
    let unmoved = auctionChannel(randomAuctionId());
    while (slotOf(unmoved) === movedSlot) {
      unmoved = auctionChannel(randomAuctionId());
    }

    const lost: string[] = [];
    subscriber.onSubscriptionLost((channel) => lost.push(channel));
    for (const channel of [...moved, unmoved]) {
      await subscriber.subscribe(channel, () => {});
    }

    await cluster.moveSlot(movedSlot, destination);

    await waitFor(() => lost.length >= moved.length, "the moved channels to be reported", 5_000);
    // One check reports everything it found in one synchronous loop, so
    // anything it wrongly reported would already be in `lost`.
    expect(lost.toSorted()).toEqual(moved.toSorted());
  });
});

// Failover exercise: the primary of a shard is replaced by its replica, as
// when it fails or is upgraded. The old primary learns of it a moment after
// the replica starts serving, and only then do its subscribers get
// `sunsubscribe` and subscribe again at the new primary. Messages published
// in between reach nobody, and nothing but the topology check reports it.
describe("a primary failing over to its replica", { timeout: 120_000 }, () => {
  const auctionsInSlot = 5;
  // The window is open for as long as the old primary has not told the
  // edge; each miss fails the shard over again.
  const maxAttemptsToLoseAnEvent = 10;
  let stub: LiveStateStub;
  let bus: TestBus;
  let cluster: TestCluster;
  let edge: EdgeRun;
  let client: SseTestClient | undefined;
  let originalPrimary: Primary;
  let currentPrimary: Primary;
  let auctionIds: string[];

  beforeEach(async () => {
    stub = new LiveStateStub();
    await stub.start();
    bus = new TestBus(topologyNodes);
    await bus.connect();
    cluster = new TestCluster(topologyNodes);
    await cluster.connect();

    const firstCandidate = Number(randomAuctionId());
    const slot = slotOf(auctionChannel(String(firstCandidate)));
    auctionIds = auctionIdsInSlot(slot, auctionsInSlot, firstCandidate);
    originalPrimary = await cluster.ownerOf(slot);
    currentPrimary = originalPrimary;

    for (const auctionId of auctionIds) {
      stub.states.set(auctionId, liveState(auctionId, 1));
    }
    edge = await startEdge({
      valkeyNodes: topologyNodes,
      auctionServiceUrl: stub.url,
      topologyCheckIntervalMs: 200,
    });
    client = await SseTestClient.open(`${edge.url}/v1/stream?auctions=${auctionIds.join(",")}`);
    for (const auctionId of auctionIds) {
      await client.waitForTimeline(auctionId, ["snapshot@1"]);
    }
  });

  afterEach(async () => {
    client?.close();
    await edge?.stop();
    // Leave the topology as it was found: the original node is the primary.
    const role = await cluster?.roleOf(originalPrimary);
    if (role !== undefined && role !== "master") {
      await cluster.promote(originalPrimary);
    }
    await cluster?.close();
    await bus?.close();
    await stub?.stop();
  });

  function latestVersions(): (string | undefined)[] {
    return auctionIds.map((auctionId) => client!.timeline(auctionId).at(-1)?.split("@")[1]);
  }

  it("leaves every client at the current version without another event", async () => {
    let lostVersion: number | undefined;
    for (let attempt = 0; attempt < maxAttemptsToLoseAnEvent; attempt += 1) {
      const version = 2 + attempt;
      for (const auctionId of auctionIds) {
        stub.states.set(auctionId, liveState(auctionId, version));
      }
      const replica = await cluster.replicaOf(currentPrimary);
      currentPrimary = await cluster.promote(replica);

      const delivered: number[] = [];
      for (const auctionId of auctionIds) {
        const message = serializeAuctionEvent(
          "BidAccepted.v1",
          bidAcceptedEvent(auctionId, version),
        );
        delivered.push(await cluster.publishTo(currentPrimary, auctionChannel(auctionId), message));
      }
      if (delivered.some((count) => count === 0)) {
        lostVersion = version;
        break;
      }
      // The edge had subscribed at the new primary before the publish, so
      // nothing was lost. Let every client reach this version and fail over
      // again.
      await expect
        .poll(latestVersions, { timeout: 5_000, interval: 50 })
        .toEqual(auctionIds.map(() => String(version)));
    }
    expect(lostVersion, "an event published before the edge subscribed again").toBeDefined();

    await expect
      .poll(latestVersions, { timeout: 10_000, interval: 50 })
      .toEqual(auctionIds.map(() => String(lostVersion)));
  });
});
