import { get, type ClientRequest, type IncomingMessage } from "node:http";

import { waitFor } from "./wait-for.js";
import type { SseMessage } from "./sse-client.types.js";

// Reads an SSE response the way a browser's EventSource would.
export class SseTestClient {
  readonly messages: SseMessage[] = [];
  ended = false;
  private pending = "";

  private constructor(
    private readonly request: ClientRequest,
    response: IncomingMessage,
  ) {
    response.setEncoding("utf8");
    response.on("data", (chunk: string) => this.receive(chunk));
    response.on("close", () => (this.ended = true));
  }

  static open(url: string): Promise<SseTestClient> {
    return new Promise((resolve, reject) => {
      const request = get(url, { headers: { accept: "text/event-stream" } }, (response) =>
        resolve(new SseTestClient(request, response)),
      );
      request.on("error", reject);
    });
  }

  // "event@version" for each message about the auction, in arrival order.
  timeline(auctionId: string): string[] {
    return this.messages
      .filter(({ data }) => data.auctionId === auctionId)
      .map(({ event, data }) =>
        typeof data.auctionVersion === "string" ? `${event}@${data.auctionVersion}` : event,
      );
  }

  async waitForTimeline(auctionId: string, expected: readonly string[]): Promise<void> {
    await waitFor(
      () => this.timeline(auctionId).length >= expected.length,
      `${expected.join(", ")} for auction ${auctionId}; got ${this.timeline(auctionId).join(", ")}`,
      30_000,
    );
  }

  close(): void {
    this.request.destroy();
  }

  private receive(chunk: string): void {
    this.pending += chunk;
    let end = this.pending.indexOf("\n\n");
    while (end !== -1) {
      const block = this.pending.slice(0, end);
      this.pending = this.pending.slice(end + 2);
      let event = "message";
      let data: string | undefined;
      for (const line of block.split("\n")) {
        if (line.startsWith("event: ")) {
          event = line.slice("event: ".length);
        } else if (line.startsWith("data: ")) {
          data = line.slice("data: ".length);
        }
      }
      if (data !== undefined) {
        this.messages.push({ event, data: JSON.parse(data) as Record<string, unknown> });
      }
      end = this.pending.indexOf("\n\n");
    }
  }
}
