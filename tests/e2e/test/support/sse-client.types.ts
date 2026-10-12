export interface SseMessage {
  event: string;
  data: Record<string, unknown>;
}
