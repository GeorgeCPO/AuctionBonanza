import { pino } from "pino";
import { describe, expect, it } from "vitest";

import { KafkaMessageTransport } from "../../src/outbox/outbox.transport.js";

describe("KafkaMessageTransport", () => {
  // The worker's shutdown path always disconnects, including after connect()
  // failed at startup. That must not add a flush error on top of the real one.
  it("disconnects quietly when it never connected", async () => {
    const transport = new KafkaMessageTransport({
      brokers: ["localhost:1"],
      clientId: "never-connected",
      publishTimeoutMs: 1_000,
      logger: pino({ level: "silent" }),
    });

    await expect(transport.disconnect(100)).resolves.toBeUndefined();
  });
});
