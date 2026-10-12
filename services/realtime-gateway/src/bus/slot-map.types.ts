// One entry of the CLUSTER SHARDS reply (node-redis 6 speaks RESP3, so maps
// arrive as plain objects). `slots` is a flat list of inclusive [start, end]
// pairs.
export interface ClusterShardReply {
  slots: number[];
  nodes: ClusterShardNodeReply[];
}

export interface ClusterShardNodeReply {
  role: string;
  port?: number;
  ip?: string;
  // The node's preferred endpoint, the one it announces to clients.
  endpoint?: string;
}

// Slots a primary owns, both ends included.
export interface SlotRange {
  start: number;
  end: number;
  // host:port of the primary.
  address: string;
}
