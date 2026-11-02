import type { LiveEntry } from "./live-auction-book.types.js";

export type StreamStatus = "idle" | "connecting" | "open" | "reconnecting";

export interface LogLine {
  id: number;
  // Local wall-clock time the frame arrived.
  receivedAt: string;
  text: string;
}

// What React reads. A new object is made on every change.
export interface LiveView {
  status: StreamStatus;
  entries: ReadonlyMap<string, LiveEntry>;
  // Server time minus local time, so countdowns follow the server's clock.
  clockOffsetMs: number;
  log: readonly LogLine[];
}

// The part of EventSource that LiveStream uses.
export interface EventSourceLike {
  addEventListener(name: string, listener: (event: MessageEvent<string>) => void): void;
  onopen: ((event: Event) => void) | null;
  onerror: ((event: Event) => void) | null;
  close(): void;
}

export type EventSourceFactory = (url: string) => EventSourceLike;

export interface LiveStreamOptions {
  createEventSource?: EventSourceFactory;
  // Replaced in tests.
  now?: () => number;
}
