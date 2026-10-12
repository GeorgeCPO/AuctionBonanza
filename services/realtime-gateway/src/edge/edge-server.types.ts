import type { Logger } from "pino";

import type { AuctionRegistry } from "./auction-registry.js";

export type StreamQuery =
  | { kind: "valid"; auctionIds: string[] }
  | { kind: "invalid"; message: string };

export interface EdgeServerOptions {
  registry: Pick<AuctionRegistry, "watch" | "unwatch" | "hasRoomFor">;
  maxAuctionsPerStream: number;
  // Streams open at once. Beyond this, new requests get 503.
  maxConnections: number;
  heartbeatIntervalMs: number;
  maxBufferedBytesPerConnection: number;
  logger: Logger;
}
