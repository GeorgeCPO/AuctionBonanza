import { createServer, connect, type Server, type Socket } from "node:net";

// Minimal TCP proxy for the broker's FAULT_INJECTION listener (see
// docker-compose.yml). The broker advertises this proxy's address, so every
// connection a client makes after bootstrapping also flows through it.
//
// cut() destroys all open connections and resets new ones immediately, which
// the Kafka client experiences as the broker being unreachable. restore()
// lets connections through again.
export class KafkaFaultProxy {
  private readonly server: Server;
  private readonly sockets = new Set<Socket>();
  private reachable = true;

  constructor(
    private readonly upstreamHost: string,
    private readonly upstreamPort: number,
  ) {
    this.server = createServer((downstream) => this.accept(downstream));
  }

  async listen(port: number): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.server.once("error", reject);
      // No host: listen on all interfaces, including both ::1 and 127.0.0.1,
      // whichever "localhost" resolves to for the client.
      this.server.listen(port, () => resolve());
    });
  }

  cut(): void {
    this.reachable = false;
    for (const socket of this.sockets) {
      socket.destroy();
    }
  }

  restore(): void {
    this.reachable = true;
  }

  async close(): Promise<void> {
    this.cut();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }

  private accept(downstream: Socket): void {
    if (!this.reachable) {
      downstream.resetAndDestroy();
      return;
    }
    const upstream = connect(this.upstreamPort, this.upstreamHost);
    for (const socket of [downstream, upstream]) {
      this.sockets.add(socket);
      socket.on("error", () => {});
      socket.on("close", () => {
        this.sockets.delete(socket);
        downstream.destroy();
        upstream.destroy();
      });
    }
    downstream.pipe(upstream);
    upstream.pipe(downstream);
  }
}
