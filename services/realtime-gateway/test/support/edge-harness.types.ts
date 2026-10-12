import type { Logger } from "pino";

import type { AuctionRegistry } from "../../src/edge/auction-registry.js";

// Holds a request until the test opens it.
export interface Gate {
  // Resolves when a request reached the gate.
  entered: Promise<void>;
  pass(): Promise<void>;
  open(): void;
}

export interface SseMessage {
  event: string;
  data: Record<string, unknown>;
}

export interface EdgeRun {
  url: string;
  registry: AuctionRegistry;
  clientName: string;
  stop(): Promise<void>;
}

export interface StartEdgeOptions {
  valkeyNodes: readonly string[];
  auctionServiceUrl: string;
  lingerMs?: number;
  heartbeatIntervalMs?: number;
  liveStateBatchDelayMs?: number;
  logger?: Logger;
}
