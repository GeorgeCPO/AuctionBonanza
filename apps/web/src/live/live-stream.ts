import { LiveAuctionBook } from "./live-auction-book.js";
import type { LiveEvent } from "./live-auction-book.types.js";
import type {
  EventSourceFactory,
  EventSourceLike,
  LiveStreamOptions,
  LiveView,
  LogLine,
} from "./live-stream.types.js";

const MAX_LOG_LINES = 100;
const FIRST_RETRY_MS = 1_000;
const MAX_RETRY_MS = 15_000;
// `snapshot` and `bid-accepted` are the SSE event names the gateway sends.
const EVENT_NAMES = ["snapshot", "bid-accepted", "unavailable"] as const;

// One SSE connection to the gateway for a set of auctions. The set is fixed
// per connection on the server, so changing it closes the stream and opens a
// new one; the server then starts again with a snapshot per auction. A dropped
// connection is reopened with backoff, and the fresh snapshots replace
// whatever was shown, so no gap handling is needed here.
export class LiveStream {
  private readonly book = new LiveAuctionBook();
  private readonly createEventSource: EventSourceFactory;
  private readonly now: () => number;
  private readonly listeners = new Set<() => void>();
  private ids: readonly string[] = [];
  private source: EventSourceLike | undefined;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private retryDelayMs = FIRST_RETRY_MS;
  private nextLogId = 1;
  private view: LiveView = { status: "idle", entries: new Map(), clockOffsetMs: 0, log: [] };

  constructor(options: LiveStreamOptions = {}) {
    this.createEventSource = options.createEventSource ?? ((url) => new EventSource(url));
    this.now = options.now ?? (() => Date.now());
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getView = (): LiveView => this.view;

  // Watches exactly `ids`. Calling it again with the same set does nothing.
  watch(ids: readonly string[]): void {
    const next = [...new Set(ids)].sort();
    const unchanged = next.length === this.ids.length && next.every((id, i) => id === this.ids[i]);
    if (unchanged) {
      return;
    }
    this.ids = next;
    this.disconnect();
    this.book.retain(new Set(next));
    if (next.length === 0) {
      this.update({ status: "idle", entries: this.book.entriesSnapshot() });
      return;
    }
    this.connect("connecting");
  }

  close(): void {
    this.ids = [];
    this.disconnect();
    this.book.retain(new Set());
    this.update({ status: "idle", entries: this.book.entriesSnapshot() });
  }

  private connect(status: "connecting" | "reconnecting"): void {
    this.update({ status });
    const source = this.createEventSource(`/gateway/v1/stream?auctions=${this.ids.join(",")}`);
    this.source = source;
    source.onopen = () => {
      this.retryDelayMs = FIRST_RETRY_MS;
      this.update({ status: "open" });
    };
    source.onerror = () => {
      // EventSource would retry by itself, but not after an HTTP error, and
      // not with a delay we control. Close it and reconnect on our own.
      this.disconnect();
      this.log(undefined, `stream lost, retrying in ${this.retryDelayMs / 1000}s`);
      this.retryTimer = setTimeout(() => this.connect("reconnecting"), this.retryDelayMs);
      this.retryDelayMs = Math.min(this.retryDelayMs * 2, MAX_RETRY_MS);
      this.update({ status: "reconnecting" });
    };
    for (const name of EVENT_NAMES) {
      source.addEventListener(name, (message) => this.handleFrame(name, message.data));
    }
  }

  private disconnect(): void {
    if (this.retryTimer !== undefined) {
      clearTimeout(this.retryTimer);
      this.retryTimer = undefined;
    }
    if (this.source !== undefined) {
      this.source.onopen = null;
      this.source.onerror = null;
      this.source.close();
      this.source = undefined;
    }
  }

  private handleFrame(name: (typeof EVENT_NAMES)[number], raw: string): void {
    let data: unknown;
    try {
      data = JSON.parse(raw);
    } catch {
      this.log(undefined, `unreadable ${name} frame`);
      return;
    }
    const event = { kind: name, data } as LiveEvent;
    const outcome = this.book.apply(event);
    this.log(event.data.auctionId, this.describe(event, outcome));

    let clockOffsetMs = this.view.clockOffsetMs;
    if (event.kind === "snapshot") {
      clockOffsetMs = Date.parse(event.data.serverTime) - this.now();
    }
    this.update({ entries: this.book.entriesSnapshot(), clockOffsetMs });
  }

  private describe(event: LiveEvent, outcome: string): string {
    switch (event.kind) {
      case "snapshot":
        return `snapshot v${event.data.auctionVersion} ${event.data.phase}`;
      case "bid-accepted":
        return `bid-accepted v${event.data.auctionVersion} ${event.data.amountMinor} (${outcome})`;
      case "unavailable":
        return `unavailable (${event.data.reason})`;
    }
  }

  private log(auctionId: string | undefined, text: string): void {
    const line: LogLine = {
      id: this.nextLogId,
      receivedAt: new Date(this.now()).toLocaleTimeString(),
      auctionId,
      text,
    };
    this.nextLogId += 1;
    this.view = { ...this.view, log: [line, ...this.view.log].slice(0, MAX_LOG_LINES) };
  }

  private update(change: Partial<LiveView>): void {
    this.view = { ...this.view, ...change };
    for (const listener of this.listeners) {
      listener();
    }
  }
}
