import { createClient } from "redis";

import { SlotMap } from "./slot-map.js";
import type { ClusterShardReply } from "./slot-map.types.js";

// Reads CLUSTER SHARDS from the first node that answers. Each attempt uses a
// short-lived connection: the answer is needed rarely, and a node that cannot
// be reached must not make the caller wait for a reconnect.
export async function readSlotMap(
  nodes: Iterable<string>,
  connectTimeoutMs: number,
): Promise<SlotMap> {
  let lastError: unknown;
  for (const address of nodes) {
    const client = createClient({
      url: `redis://${address}`,
      socket: { connectTimeout: connectTimeoutMs, reconnectStrategy: false },
    });
    client.on("error", () => undefined);
    try {
      await client.connect();
      const shards = (await client.sendCommand(["CLUSTER", "SHARDS"])) as ClusterShardReply[];
      return SlotMap.fromShards(shards);
    } catch (error) {
      lastError = error;
    } finally {
      client.destroy();
    }
  }
  throw new Error("No cluster node answered CLUSTER SHARDS", { cause: lastError });
}
