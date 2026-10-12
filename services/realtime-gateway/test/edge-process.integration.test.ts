import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  liveState,
  LiveStateStub,
  randomAuctionId,
  SseTestClient,
} from "./support/edge-harness.js";
import { waitFor } from "./support/relay-harness.js";
import {
  compileService,
  gatewayRoot,
  startServiceProcess,
  type CompiledService,
} from "./support/service-process.js";

const valkeyNodes = process.env.TEST_VALKEY_NODES;

// Runs the built edge as its own OS process, exactly as it is deployed, with
// live states from a stub of auction-service. The path through the real
// auction-service and relay is covered by the root end-to-end suite
// (tests/e2e).
describe.skipIf(valkeyNodes === undefined)("edge process", { timeout: 120_000 }, () => {
  let gateway: CompiledService | undefined;
  let stub: LiveStateStub | undefined;

  beforeAll(async () => {
    gateway = await compileService(gatewayRoot, "edge-test");
    stub = new LiveStateStub();
    await stub.start();
  });

  afterAll(async () => {
    await stub?.stop();
    await gateway?.remove();
  });

  it("refuses to start with invalid configuration", async () => {
    const edge = startServiceProcess(gateway!, "edge.main.js", {
      AUCTION_SERVICE_URL: stub!.url,
    });

    expect(await edge.exited).toEqual([1, null]);
    expect(edge.stderr()).toContain("Invalid edge configuration: /valkeyNodes");
  });

  it("serves a stream, then ends it and exits cleanly on SIGTERM", async () => {
    const auctionId = randomAuctionId();
    stub!.states.set(auctionId, liveState(auctionId, 1));
    const edge = startServiceProcess(gateway!, "edge.main.js", {
      HOST: "127.0.0.1",
      PORT: "0",
      VALKEY_NODES: valkeyNodes!,
      AUCTION_SERVICE_URL: stub!.url,
    });
    const { port } = await edge.waitForLog("edge started");
    const client = await SseTestClient.open(
      `http://127.0.0.1:${String(port)}/v1/stream?auctions=${auctionId}`,
    );
    await client.waitForTimeline(auctionId, ["snapshot@1"]);

    expect(await edge.stop()).toEqual([0, null]);

    await waitFor(() => client.ended, "the stream to end");
    const messages = edge.logs().map(({ msg }) => msg);
    expect(messages).toContain("shutdown requested");
    expect(messages.at(-1)).toBe("edge stopped");
  });
});
