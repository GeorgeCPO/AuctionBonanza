import type { Logger } from "pino";
import { createCluster } from "redis";

import type { BusPublisher } from "./bus.js";

export interface ValkeyBusPublisherOptions {
  // Seed nodes as host:port; the client discovers the rest of the cluster.
  nodes: readonly string[];
  // Bounds one publish, including time spent queued while a node reconnects.
  // A publish that times out rejects, so the relay retries it instead of
  // waiting on a dead connection.
  commandTimeoutMs: number;
  logger: Logger;
}

// Publishes with SPUBLISH through node-redis's cluster client, which routes
// each channel to the primary owning its hash slot and follows MOVED
// redirects. The relay needs no more than that: a lost publish surfaces as a
// rejected command, never silently.
export class ValkeyBusPublisher implements BusPublisher {
  private readonly cluster: ReturnType<typeof createCluster>;
  private connected = false;

  constructor(options: ValkeyBusPublisherOptions) {
    const logger = options.logger.child({ component: "valkey" });
    this.cluster = createCluster({
      rootNodes: options.nodes.map((node) => ({ url: `redis://${node}` })),
      commandOptions: { timeout: options.commandTimeoutMs },
      defaults: {
        socket: {
          connectTimeout: options.commandTimeoutMs,
          // Reconnect quickly and indefinitely; the relay's own backoff
          // decides how often a failed publish is retried.
          reconnectStrategy: (retries: number) => Math.min(50 * 2 ** retries, 1_000),
        },
      },
    });
    // Without listeners, node-redis re-emits connection errors as unhandled
    // 'error' events, which would crash the process.
    this.cluster.on("error", (error: unknown) =>
      logger.error({ err: error }, "Valkey cluster error"),
    );
    this.cluster.on("node-error", (error: unknown, node: unknown) =>
      logger.warn({ err: error, node }, "Valkey node error"),
    );
  }

  async connect(): Promise<void> {
    await this.cluster.connect();
    this.connected = true;
  }

  async publish(channel: string, message: string): Promise<void> {
    await this.cluster.sPublish(channel, message);
  }

  async close(): Promise<void> {
    if (!this.connected) return;
    this.connected = false;
    await this.cluster.close();
  }
}
