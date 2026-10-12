import { randomUUID } from "node:crypto";

import { createClient, createCluster } from "redis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import type { ClusterShardReply } from "../../src/bus/slot-map.types.js";
import { requireEnvironment } from "../support/environment.js";

// The edge's shard subscriber depends on three properties of the Compose
// cluster, checked here from the host the way the gateway will use it:
// CLUSTER SHARDS lists every primary with an endpoint the host can reach,
// the primaries cover every slot, and a sharded message published through
// the cluster client reaches a subscriber connected to the owning primary.
const valkeyNodes = requireEnvironment("TEST_VALKEY_NODES").split(",");

interface Primary {
  endpoint: string;
  port: number;
  slotRanges: [number, number][];
}

describe("Valkey cluster", () => {
  let cluster: ReturnType<typeof createCluster>;
  let primaries: Primary[];

  async function readPrimaries(): Promise<Primary[]> {
    const client = createClient({ url: `redis://${valkeyNodes[0]}` });
    await client.connect();
    try {
      const shards = (await client.sendCommand(["CLUSTER", "SHARDS"])) as ClusterShardReply[];
      return shards.map(({ slots, nodes }) => {
        const primary = nodes.find((node) => node.role === "master" || node.role === "primary");
        if (primary === undefined) {
          throw new Error("Shard has no primary");
        }

        // Slots arrive as a flat list of inclusive [start, end] pairs.
        const slotRanges: [number, number][] = [];
        for (let index = 0; index + 1 < slots.length; index += 2) {
          slotRanges.push([slots[index]!, slots[index + 1]!]);
        }
        if (primary.endpoint === undefined || primary.port === undefined) {
          throw new Error("Primary has no endpoint or port");
        }
        return { endpoint: primary.endpoint, port: primary.port, slotRanges };
      });
    } finally {
      await client.close();
    }
  }

  beforeAll(async () => {
    cluster = createCluster({
      rootNodes: valkeyNodes.map((node) => ({ url: `redis://${node}` })),
    });
    await cluster.connect();
    primaries = await readPrimaries();
  });

  afterAll(async () => {
    await cluster?.close();
  });

  it("announces a host-reachable endpoint for every primary and covers every slot", () => {
    expect(primaries.map(({ endpoint, port }) => `${endpoint}:${port}`).toSorted()).toEqual(
      valkeyNodes.toSorted(),
    );

    const ranges = primaries
      .flatMap((primary) => primary.slotRanges)
      .toSorted((a, b) => a[0] - b[0]);
    let nextSlot = 0;
    for (const [start, end] of ranges) {
      expect(start).toBe(nextSlot);
      nextSlot = end + 1;
    }
    expect(nextSlot).toBe(16_384);
  });

  it("delivers a sharded message published through the cluster to the owning primary's subscriber", async () => {
    for (const primary of primaries) {
      const channel = await channelOwnedBy(primary);
      const subscriber = createClient({ url: `redis://${primary.endpoint}:${primary.port}` });
      await subscriber.connect();
      try {
        let deliver: (message: string) => void = () => {};
        const received = new Promise<string>((resolve) => {
          deliver = resolve;
        });
        // sSubscribe resolves once the server confirms the subscription.
        // Publishing earlier could lose the message: pub/sub keeps no history.
        await subscriber.sSubscribe(channel, (message) => deliver(message));

        const message = randomUUID();
        await expect(cluster.sPublish(channel, message)).resolves.toBe(1);
        await expect(received).resolves.toBe(message);
      } finally {
        await subscriber.close();
      }
    }
  });

  // Finds an auction channel whose hash slot the given primary owns.
  async function channelOwnedBy(primary: Primary): Promise<string> {
    const client = createClient({ url: `redis://${primary.endpoint}:${primary.port}` });
    await client.connect();
    try {
      for (let auctionId = 1; auctionId < 10_000; auctionId += 1) {
        const channel = `auction:${auctionId}`;
        const slot = Number(await client.sendCommand(["CLUSTER", "KEYSLOT", channel]));
        if (primary.slotRanges.some(([start, end]) => slot >= start && slot <= end)) {
          return channel;
        }
      }
      throw new Error(`No channel found for ${primary.endpoint}:${primary.port}`);
    } finally {
      await client.close();
    }
  }
});
