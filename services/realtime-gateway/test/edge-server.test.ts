import { get } from "node:http";
import { connect } from "node:net";

import { pino } from "pino";
import { afterEach, describe, expect, it } from "vitest";

import type { AuctionWatcher } from "../src/edge/auction-registry.types.js";
import { EdgeServer } from "../src/edge/edge-server.js";
import type { EdgeServerOptions } from "../src/edge/edge-server.types.js";
import { waitFor } from "./support/relay-harness.js";

// Larger than any socket buffer between two local processes, so most of it
// stays unwritten while the client does not read.
const HUGE_SNAPSHOT = `event: snapshot\ndata: "${"x".repeat(32 * 1024 * 1024)}"\n\n`;

// A registry that accepts everything and sends nothing.
const idleRegistry: EdgeServerOptions["registry"] = {
  watch: () => {},
  unwatch: () => {},
  hasRoomFor: () => true,
};

function serverWith(overrides: Partial<EdgeServerOptions> = {}): EdgeServer {
  return new EdgeServer({
    registry: idleRegistry,
    maxAuctionsPerStream: 50,
    maxConnections: 100,
    heartbeatIntervalMs: 60_000,
    maxBufferedBytesPerConnection: 1024 * 1024,
    logger: pino({ level: "silent" }),
    ...overrides,
  });
}

// Sends the request line as it is, because an HTTP client would refuse or
// rewrite a target like "//", and returns the first line of the response.
function statusLineOf(port: number, requestTarget: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const socket = connect(port, "127.0.0.1");
    let received = "";
    socket.on("data", (chunk: Buffer) => (received += chunk.toString("utf8")));
    socket.on("error", reject);
    socket.on("close", () => resolve(received.split("\r\n")[0] ?? ""));
    socket.write(`GET ${requestTarget} HTTP/1.1\r\nHost: edge\r\nConnection: close\r\n\r\n`);
  });
}

describe("EdgeServer", () => {
  let server: EdgeServer | undefined;

  afterEach(async () => {
    await server?.close();
  });

  it("stops watching a stream's remaining auctions once an opening snapshot overflows it", async () => {
    const watched: string[] = [];
    const unwatched: string[] = [];
    server = serverWith({
      registry: {
        watch: (auctionId: string, watcher: AuctionWatcher) => {
          watched.push(auctionId);
          watcher.send(HUGE_SNAPSHOT);
        },
        unwatch: (auctionId: string) => unwatched.push(auctionId),
        hasRoomFor: () => true,
      },
      maxBufferedBytesPerConnection: 1024,
    });
    const { port } = await server.listen("127.0.0.1", 0);

    // The client never reads the body.
    const request = get(`http://127.0.0.1:${port}/v1/stream?auctions=1,2,3`, (response) =>
      response.pause(),
    );
    request.on("error", () => {});
    await waitFor(() => unwatched.length === 3, "the overflowing stream to close", 5_000);
    request.destroy();

    expect(watched).toEqual(["1"]);
    expect(unwatched).toEqual(["1", "2", "3"]);
    expect(server.connectionCount).toBe(0);
  });

  // `new URL` throws on these targets. An exception in the request handler
  // ended the whole process, and every client's stream with it.
  it.each(["//", "//[", "http://["])(
    "answers 400 to the request target %j and keeps serving",
    async (requestTarget) => {
      server = serverWith();
      const { port } = await server.listen("127.0.0.1", 0);

      const statusLine = await statusLineOf(port, requestTarget);
      const health = await fetch(`http://127.0.0.1:${port}/health`);

      expect(statusLine).toBe("HTTP/1.1 400 Bad Request");
      expect(health.status).toBe(200);
    },
  );

  it("answers 503 when the edge has no room for another stream", async () => {
    server = serverWith({ maxConnections: 1 });
    const { port } = await server.listen("127.0.0.1", 0);
    const first = get(`http://127.0.0.1:${port}/v1/stream?auctions=1`, (response) =>
      response.resume(),
    );
    first.on("error", () => {});
    await waitFor(() => server!.connectionCount === 1, "the first stream to open");

    const second = await fetch(`http://127.0.0.1:${port}/v1/stream?auctions=2`);
    first.destroy();

    expect(second.status).toBe(503);
    expect(await second.json()).toEqual({ code: "OVER_CAPACITY", message: "over capacity" });
  });

  it("answers 503 when the registry has no room for the requested auctions", async () => {
    const asked: string[][] = [];
    server = serverWith({
      registry: {
        ...idleRegistry,
        hasRoomFor: (auctionIds) => {
          asked.push([...auctionIds]);
          return false;
        },
      },
    });
    const { port } = await server.listen("127.0.0.1", 0);

    const response = await fetch(`http://127.0.0.1:${port}/v1/stream?auctions=1,2`);

    expect(response.status).toBe(503);
    expect(asked).toEqual([["1", "2"]]);
    expect(server.connectionCount).toBe(0);
  });
});
