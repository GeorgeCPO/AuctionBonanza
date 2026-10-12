import type { Logger } from "pino";

import type { AuctionRegistry } from "./auction-registry.js";

export type StreamQuery =
  | { kind: "valid"; auctionIds: string[] }
  | { kind: "invalid"; message: string };

export interface EdgeServerOptions {
  registry: Pick<AuctionRegistry, "watch" | "unwatch">;
  maxAuctionsPerStream: number;
  heartbeatIntervalMs: number;
  maxBufferedBytesPerConnection: number;
  logger: Logger;
}
