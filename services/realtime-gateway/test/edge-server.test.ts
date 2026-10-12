import { get } from "node:http";

import { pino } from "pino";
import { afterEach, describe, expect, it } from "vitest";

import type { AuctionWatcher } from "../src/edge/auction-registry.types.js";
import { EdgeServer } from "../src/edge/edge-server.js";
import { waitFor } from "./support/relay-harness.js";

// Larger than any socket buffer between two local processes, so most of it
// stays unwritten while the client does not read.
const HUGE_SNAPSHOT = `event: snapshot\ndata: "${"x".repeat(32 * 1024 * 1024)}"\n\n`;

describe("EdgeServer", () => {
  let server: EdgeServer | undefined;

  afterEach(async () => {
    await server?.close();
  });

  it("stops watching a stream's remaining auctions once an opening snapshot overflows it", async () => {
    const watched: string[] = [];
    const unwatched: string[] = [];
    server = new EdgeServer({
      registry: {
        watch: (auctionId: string, watcher: AuctionWatcher) => {
          watched.push(auctionId);
          watcher.send(HUGE_SNAPSHOT);
        },
        unwatch: (auctionId: string) => unwatched.push(auctionId),
      },
      maxAuctionsPerStream: 50,
      heartbeatIntervalMs: 60_000,
      maxBufferedBytesPerConnection: 1024,
      logger: pino({ level: "silent" }),
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
});
