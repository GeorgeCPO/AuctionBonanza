// The fields every outbox event contract carries; the whole event becomes the
// stored payload.
export interface OutboxEventInput {
  eventId: string;
  eventType: string;
  occurredAt: string;
  auctionId: string;
  auctionVersion: string;
}
