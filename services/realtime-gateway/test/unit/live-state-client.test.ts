import { afterEach, describe, expect, it, vi } from "vitest";

import { LiveStateClient } from "../../src/edge/live-states.js";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("LiveStateClient", () => {
  it("cancels the body of an error response, so its connection is released", async () => {
    const cancel = vi.fn<() => void>();
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(new ReadableStream({ cancel }), { status: 503 }),
    );
    const client = new LiveStateClient({
      auctionServiceUrl: "http://auction.test",
      timeoutMs: 1_000,
    });

    await expect(client.fetch(["1"])).rejects.toThrow("HTTP 503");

    expect(cancel).toHaveBeenCalledTimes(1);
  });
});
