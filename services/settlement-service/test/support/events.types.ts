export interface RecordOptions {
  // Defaults to the event's auctionId, as the outbox publisher sets it.
  key?: string | null;
  // Defaults to the event's type and id. A header set to undefined is absent.
  eventType?: string | undefined;
  eventId?: string | undefined;
  value?: string | null;
  partition?: number;
  offset?: string;
}
