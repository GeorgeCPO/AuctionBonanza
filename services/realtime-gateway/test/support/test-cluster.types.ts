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

// One entry of the CLUSTER SHARDS reply (node-redis 6 speaks RESP3, so maps
// arrive as plain objects). `slots` is a flat list of inclusive [start, end]
// pairs.
export interface ClusterShard {
  slots: number[];
  nodes: { id: string; role: string }[];
}
