import { Writable } from "node:stream";

import { pino, type Logger } from "pino";

// Collects log lines so tests can assert on what the worker reported.
export function captureLogger(): { logger: Logger; lines: Record<string, unknown>[] } {
  const lines: Record<string, unknown>[] = [];
  const stream = new Writable({
    write(chunk: Buffer, _encoding, callback) {
      lines.push(JSON.parse(chunk.toString("utf8")) as Record<string, unknown>);
      callback();
    },
  });
  return { logger: pino({ level: "debug" }, stream), lines };
}
