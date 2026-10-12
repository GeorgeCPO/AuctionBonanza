import { once } from "node:events";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import type { Logger } from "pino";

import type { EdgeServerOptions, StreamQuery } from "./edge-server.types.js";
import { SseConnection } from "./sse.js";

// PostgreSQL bigint, the type of auction ids.
const MAX_AUCTION_ID = 9_223_372_036_854_775_807n;
const auctionIdPattern = /^[1-9][0-9]{0,18}$/;

// GET /v1/stream?auctions=1,2,3: one to `maxAuctions` distinct auction ids.
function parseStreamQuery(query: URLSearchParams, maxAuctions: number): StreamQuery {
  const invalid: StreamQuery = {
    kind: "invalid",
    message: `auctions must be 1 to ${maxAuctions} comma-separated auction IDs`,
  };
  const keys = [...query.keys()];
  if (keys.length !== 1 || keys[0] !== "auctions") {
    return invalid;
  }

  const auctionIds = [...new Set(query.get("auctions")!.split(","))];
  if (auctionIds.length > maxAuctions) {
    return invalid;
  }
  const valid = auctionIds.every((id) => auctionIdPattern.test(id) && BigInt(id) <= MAX_AUCTION_ID);
  return valid ? { kind: "valid", auctionIds } : invalid;
}

// The edge's HTTP surface: the public SSE stream and a health check. Plain
// node:http, because a stream needs direct control over the response and its
// write buffer, and there is nothing else to route.
export class EdgeServer {
  private readonly server: Server;
  private readonly connections = new Set<SseConnection>();
  private readonly logger: Logger;

  constructor(private readonly options: EdgeServerOptions) {
    this.logger = options.logger.child({ component: "edge-server" });
    this.server = createServer((request, response) => this.handle(request, response));
  }

  get connectionCount(): number {
    return this.connections.size;
  }

  async listen(host: string, port: number): Promise<AddressInfo> {
    this.server.listen(port, host);
    await once(this.server, "listening");
    return this.server.address() as AddressInfo;
  }

  // Stops accepting connections and ends every stream. Clients reconnect
  // through the load balancer to another edge.
  async close(): Promise<void> {
    if (!this.server.listening) {
      return;
    }
    const closed = new Promise<void>((resolve, reject) =>
      this.server.close((error) => (error === undefined ? resolve() : reject(error))),
    );
    for (const connection of this.connections) {
      connection.close();
    }
    // A client that stopped reading would hold its socket open until the
    // shutdown deadline; it reconnects either way.
    this.server.closeAllConnections();
    await closed;
  }

  private handle(request: IncomingMessage, response: ServerResponse): void {
    const url = this.parseUrl(request);
    if (url === undefined) {
      return this.json(response, 400, {
        code: "INVALID_REQUEST",
        message: "request target is not valid",
      });
    }
    if (url.pathname === "/health") {
      if (request.method !== "GET") {
        return this.reply(response, 405, "METHOD_NOT_ALLOWED");
      }
      return this.json(response, 200, { status: "ok" });
    }
    if (url.pathname !== "/v1/stream") {
      return this.reply(response, 404, "NOT_FOUND");
    }
    if (request.method !== "GET") {
      return this.reply(response, 405, "METHOD_NOT_ALLOWED");
    }

    const query = parseStreamQuery(url.searchParams, this.options.maxAuctionsPerStream);
    if (query.kind === "invalid") {
      return this.json(response, 400, { code: "INVALID_REQUEST", message: query.message });
    }
    if (this.connections.size >= this.options.maxConnections) {
      return this.reply(response, 503, "OVER_CAPACITY");
    }
    if (!this.options.registry.hasRoomFor(query.auctionIds)) {
      return this.reply(response, 503, "OVER_CAPACITY");
    }
    this.openStream(response, query.auctionIds);
  }

  private openStream(response: ServerResponse, auctionIds: readonly string[]): void {
    const { registry } = this.options;
    const connection: SseConnection = new SseConnection(response, {
      heartbeatIntervalMs: this.options.heartbeatIntervalMs,
      maxBufferedBytes: this.options.maxBufferedBytesPerConnection,
      onOverflow: () => {
        this.logger.warn(
          { auctions: auctionIds.length },
          "closing a stream whose client reads too slowly",
        );
      },
      onClose: () => {
        this.connections.delete(connection);
        for (const auctionId of auctionIds) {
          registry.unwatch(auctionId, connection);
        }
      },
    });
    this.connections.add(connection);
    // Each watch sends the auction's snapshot right away when the edge
    // already holds it, so the client starts from one snapshot per auction.
    // A snapshot can overflow a client that reads too slowly; its connection
    // is then closed and already unwatched, so it must not watch the rest.
    for (const auctionId of auctionIds) {
      if (connection.isClosed) {
        break;
      }
      registry.watch(auctionId, connection);
    }
  }

  // The request target is client input, and `new URL` throws on some valid
  // ones (for example "//"). An exception here would end the process.
  private parseUrl(request: IncomingMessage): URL | undefined {
    try {
      return new URL(request.url ?? "/", "http://edge");
    } catch {
      return undefined;
    }
  }

  private reply(response: ServerResponse, status: number, code: string): void {
    this.json(response, status, { code, message: code.toLowerCase().replaceAll("_", " ") });
  }

  private json(response: ServerResponse, status: number, body: unknown): void {
    response.writeHead(status, { "content-type": "application/json; charset=utf-8" });
    response.end(JSON.stringify(body));
  }
}
