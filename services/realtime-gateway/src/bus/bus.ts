// The realtime gateway's internal pub/sub contract between the relay and the
// edges. It is owned by this service, not by packages/contracts: nothing
// outside the gateway publishes or subscribes to these channels.
//
// See "Bus" in docs/REALTIME_GATEWAY.md.

export const BUS_ENVELOPE_VERSION = 1;

// Broadcast channel for instructions to every edge. Low traffic; it lives in
// one hash slot like any other sharded channel.
export const CONTROL_CHANNEL = "gateway:control";

export function auctionChannel(auctionId: string): string {
  return `auction:${auctionId}`;
}

// Pub/sub has no headers, so the event type travels inside the envelope. The
// event is the Kafka payload as it was validated by its contract.
export interface AuctionEventEnvelope {
  v: typeof BUS_ENVELOPE_VERSION;
  eventType: string;
  event: unknown;
}

// Tells every edge to refresh all of its auctions from auction-service. The
// relay sends it when it may have skipped events, for example after starting
// a partition from the latest offset.
export interface ResyncControlMessage {
  v: typeof BUS_ENVELOPE_VERSION;
  type: "resync";
  reason: string;
}

// Publishing resolves once the bus accepted the message. Delivery to
// subscribers is at most once; see "Loss is observable" in the design.
export interface BusPublisher {
  publish(channel: string, message: string): Promise<void>;
}

export function serializeAuctionEvent(eventType: string, event: unknown): string {
  const envelope: AuctionEventEnvelope = { v: BUS_ENVELOPE_VERSION, eventType, event };
  return JSON.stringify(envelope);
}

export function serializeResync(reason: string): string {
  const message: ResyncControlMessage = { v: BUS_ENVELOPE_VERSION, type: "resync", reason };
  return JSON.stringify(message);
}
