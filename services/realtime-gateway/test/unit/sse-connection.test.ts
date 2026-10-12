import { EventEmitter } from "node:events";
import type { ServerResponse } from "node:http";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { HEARTBEAT_FRAME, SseConnection, sseEvent } from "../../src/edge/sse.js";

// A response whose client never reads: every write stays buffered.
class StalledResponse extends EventEmitter {
  readonly written: string[] = [];
  writableLength = 0;
  destroyed = false;
  ended = false;
  status: number | undefined;

  writeHead(status: number): this {
    this.status = status;
    return this;
  }

  flushHeaders(): void {}

  write(chunk: string): boolean {
    this.written.push(chunk);
    this.writableLength += Buffer.byteLength(chunk);
    return false;
  }

  destroy(): void {
    this.destroyed = true;
  }

  end(): void {
    this.ended = true;
  }
}

function open(response: StalledResponse, maxBufferedBytes = 1_024) {
  const events: string[] = [];
  const connection = new SseConnection(response as unknown as ServerResponse, {
    heartbeatIntervalMs: 20,
    maxBufferedBytes,
    onOverflow: () => events.push("overflow"),
    onClose: () => events.push("close"),
  });
  return { connection, events };
}

describe("SseConnection", () => {
  // Fake timers keep the heartbeat deterministic: with real ones, a loaded
  // machine could delay the interval and the test would count too few frames.
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("formats events as single-line SSE frames", () => {
    expect(sseEvent("snapshot", { auctionId: "42", note: "two\nlines" })).toBe(
      'event: snapshot\ndata: {"auctionId":"42","note":"two\\nlines"}\n\n',
    );
  });

  it("closes a connection whose unwritten bytes exceed the bound, and writes nothing after", () => {
    const response = new StalledResponse();
    const { connection, events } = open(response);
    const frame = sseEvent("bid-accepted", { padding: "x".repeat(400) });

    connection.send(frame);
    connection.send(frame);
    expect(events).toEqual([]);
    connection.send(frame);
    connection.send(frame);

    expect(events).toEqual(["overflow", "close"]);
    expect(response.destroyed).toBe(true);
    expect(response.written).toHaveLength(3);
  });

  it("sends heartbeats until the client disconnects", async () => {
    const response = new StalledResponse();
    const { events } = open(response, 1_000_000);

    // The interval is 20 ms, so it fires at 20, 40 and 60 ms.
    await vi.advanceTimersByTimeAsync(70);
    response.emit("close");
    const heartbeats = response.written.length;
    await vi.advanceTimersByTimeAsync(50);

    expect(response.status).toBe(200);
    expect(heartbeats).toBe(3);
    expect(new Set(response.written)).toEqual(new Set([HEARTBEAT_FRAME]));
    expect(response.written).toHaveLength(heartbeats);
    expect(events).toEqual(["close"]);
  });
});
