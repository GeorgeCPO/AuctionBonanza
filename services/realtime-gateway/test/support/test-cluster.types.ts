import type { ClusterShardReply } from "../../src/bus/slot-map.types.js";

// A primary of the test cluster, as the cluster knows it.
export interface Primary {
  address: string;
  id: string;
}

// The part of a node-redis client the helper uses.
export interface PrimaryClient {
  sendCommand(args: string[]): Promise<unknown>;
  close(): Promise<void>;
}

export interface PrimaryConnection extends Primary {
  client: PrimaryClient;
}

// One entry of the CLUSTER SHARDS reply, as the gateway reads it.
export type ClusterShard = ClusterShardReply;
