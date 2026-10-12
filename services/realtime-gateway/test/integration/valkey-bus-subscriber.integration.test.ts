import { randomUUID } from "node:crypto";

import { pino } from "pino";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { auctionChannel } from "../../src/bus/bus.js";
import { ValkeyBusPublisher } from "../../src/bus/valkey-bus-publisher.js";
import { ValkeyBusSubscriber } from "../../src/bus/valkey-bus-subscriber.js";
import { killSubscriberConnections, randomAuctionId } from "../support/edge-harness.js";
import { waitFor } from "../support/relay-harness.js";
import { requireEnvironment } from "../support/environment.js";

const valkeyNodes = requireEnvironment("TEST_VALKEY_NODES").split(",");

// The subscriber's one promise to the registry: once whenRestored() resolves
// after an interruption, every channel is subscribed again, so a message
// published from then on is delivered. The registry relies on it to read
// live states only when no event can fall between the read and the bus.
describe("ValkeyBusSubscriber", { timeout: 30_000 }, () => {
  const clientName = `subscriber-test-${randomUUID()}`;
  let subscriber: ValkeyBusSubscriber;
  let publisher: ValkeyBusPublisher;

  beforeAll(async () => {
    const logger = pino({ level: "silent" });
    subscriber = new ValkeyBusSubscriber({
      nodes: valkeyNodes,
      clientName,
      connectTimeoutMs: 2_000,
      topologyCheckIntervalMs: 60_000,
      logger,
    });
    await subscriber.connect();
    publisher = new ValkeyBusPublisher({ nodes: valkeyNodes, commandTimeoutMs: 2_000, logger });
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

    const killed = await killSubscriberConnections(valkeyNodes, clientName);
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
