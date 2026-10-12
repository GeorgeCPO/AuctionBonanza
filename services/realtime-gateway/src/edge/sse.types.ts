export interface SseConnectionOptions {
  heartbeatIntervalMs: number;
  // Unwritten bytes allowed before the connection is closed. The client
  // reconnects and starts again from snapshots.
  maxBufferedBytes: number;
  onOverflow: () => void;
  onClose: () => void;
}
