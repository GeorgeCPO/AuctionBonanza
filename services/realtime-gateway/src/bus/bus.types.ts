import type { BidAcceptedV1 } from "@auction-bonanza/contracts";
import type { Logger } from "pino";
import type { createCluster } from "redis";

// Pub/sub has no headers, so the event type travels inside the envelope. The
// event is the Kafka payload as it was validated by its contract.
export interface AuctionEventEnvelope {
  // BUS_ENVELOPE_VERSION. serializeAuctionEvent assigns the constant to this
  // field, so the two cannot drift apart without a compile error.
  v: 1;
  eventType: string;
  event: unknown;
}

// Publishing resolves once the bus accepted the message. Delivery to
// subscribers is at most once; see "Loss is observable" in the design.
export interface BusPublisher {
  publish(channel: string, message: string): Promise<void>;
}

// The edge's side of the bus. Delivery is at most once, so a subscriber must
// report every moment its subscriptions may have lost messages: the edge
// resyncs every auction it watches when that happens.
export interface BusSubscriber {
  // Resolves once the server confirmed the subscription. Messages published
  // from then on reach the listener while the connection stays up.
  subscribe(channel: string, listener: (message: string) => void): Promise<void>;
  unsubscribe(channel: string): Promise<void>;
  // Called when a connection carrying subscriptions was lost.
  onInterruption(listener: () => void): void;
  // Called when one channel's listener was dropped without the connection
  // being lost, so the channel must be subscribed again.
  onSubscriptionLost(listener: (channel: string) => void): void;
  // Resolves once every lost connection is back and has resubscribed. A
  // resync reads the live state only after this, so that any event committed
  // after the read is delivered.
  whenRestored(): Promise<void>;
}

// An auction event as an edge reads it from the bus. Every event carries the
// auction version it produced, so even an event the edge cannot apply tells
// it whether it is behind.
export type BusAuctionEvent =
  | { kind: "bid-accepted"; auctionId: string; version: bigint; event: BidAcceptedV1 }
  | { kind: "unsupported"; auctionId: string; version: bigint; eventType: string };

export interface ValkeyBusPublisherOptions {
  // Seed nodes as host:port; the client discovers the rest of the cluster.
  nodes: readonly string[];
  // Bounds one publish, including time spent queued while a node reconnects.
  // A publish that times out rejects, so the relay retries it instead of
  // waiting on a dead connection.
  commandTimeoutMs: number;
  logger: Logger;
}

export interface ValkeyBusSubscriberOptions {
  // Seed nodes as host:port; the client discovers the rest of the cluster.
  nodes: readonly string[];
  // Shown by CLIENT LIST, so operators (and tests) can find the edge's
  // connections.
  clientName: string;
  connectTimeoutMs: number;
  // How often CLUSTER SHARDS is compared with the owners the watched channels
  // were subscribed at. It bounds how long a slot move that node-redis
  // followed silently goes unreported.
  topologyCheckIntervalMs: number;
  logger: Logger;
}

// One primary's subscriber connection inside node-redis's cluster client.
export type ShardSubscriberClient = NonNullable<
  ReturnType<typeof createCluster>["masters"][number]["pubSub"]
>["client"];
