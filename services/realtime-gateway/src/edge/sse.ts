import type { ServerResponse } from "node:http";

import type { SseConnectionOptions } from "./sse.types.js";

// One SSE message. JSON.stringify escapes line breaks, so the data always
// fits on the single `data:` line the frame needs.
export function sseEvent(name: string, data: unknown): string {
  return `event: ${name}\ndata: ${JSON.stringify(data)}\n\n`;
}

// A comment line: clients ignore it, but it keeps load balancers and proxies
// from closing a quiet stream.
export const HEARTBEAT_FRAME = ": heartbeat\n\n";

// One client's event stream. It writes frames as they come and closes the
// connection when the client reads too slowly, so a slow reader cannot grow
// the edge's memory. Conflating instead of closing is a later step; see
// "Slow clients" in docs/REALTIME_GATEWAY.md.
export class SseConnection {
  private readonly heartbeat: NodeJS.Timeout;
  private closed = false;

  constructor(
    private readonly response: ServerResponse,
    private readonly options: SseConnectionOptions,
  ) {
    response.writeHead(200, {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
      // Stops nginx-style proxies from buffering the stream.
      "x-accel-buffering": "no",
    });
    response.flushHeaders();
    response.on("close", () => {
      if (!this.closed) this.finish();
    });
    this.heartbeat = setInterval(() => this.send(HEARTBEAT_FRAME), options.heartbeatIntervalMs);
  }

  get isClosed(): boolean {
    return this.closed;
  }

  send(frame: string): void {
    if (this.closed) return;
    this.response.write(frame);
    if (this.response.writableLength > this.options.maxBufferedBytes) {
      this.options.onOverflow();
      this.response.destroy();
      this.finish();
    }
  }

  // Ends the stream from the server side, for example on shutdown.
  close(): void {
    if (this.closed) return;
    this.response.end();
    this.finish();
  }

  private finish(): void {
    this.closed = true;
    clearInterval(this.heartbeat);
    this.options.onClose();
  }
}
