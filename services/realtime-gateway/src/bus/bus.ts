// The realtime gateway's internal pub/sub contract between the relay and the
// edges. It is owned by this service, not by packages/contracts: nothing
// outside the gateway publishes or subscribes to these channels.
//
// See "Bus" in docs/REALTIME_GATEWAY.md.

export const BUS_ENVELOPE_VERSION = 1;

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

// Publishing resolves once the bus accepted the message. Delivery to
// subscribers is at most once; see "Loss is observable" in the design.
export interface BusPublisher {
  publish(channel: string, message: string): Promise<void>;
}

export function serializeAuctionEvent(eventType: string, event: unknown): string {
  const envelope: AuctionEventEnvelope = { v: BUS_ENVELOPE_VERSION, eventType, event };
  return JSON.stringify(envelope);
}
