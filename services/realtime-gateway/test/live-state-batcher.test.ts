import { describe, expect, it } from "vitest";

import { LiveStateBatcher } from "../src/edge/live-states.js";
import type { LiveStateLookup } from "../src/edge/live-states.types.js";
import { liveState } from "./support/edge-harness.js";
import { waitFor } from "./support/relay-harness.js";

// Answers every requested id as found, except the ones listed as missing or
// omitted. Requests stay open until the test releases them.
class FakeClient {
  readonly requests: string[][] = [];
  readonly missing = new Set<string>();
  readonly omitted = new Set<string>();
  private releases: (() => void)[] = [];
  holdRequests = false;

  async fetch(auctionIds: readonly string[]): Promise<Map<string, LiveStateLookup>> {
    this.requests.push([...auctionIds]);
    if (this.holdRequests) {
      await new Promise<void>((resolve) => this.releases.push(resolve));
    }
    return new Map(
      auctionIds
        .filter((id) => !this.omitted.has(id))
        .map((id): [string, LiveStateLookup] => [
          id,
          this.missing.has(id) ? { kind: "missing" } : { kind: "found", state: liveState(id, 1) },
        ]),
    );
  }

  releaseAll(): void {
    for (const release of this.releases) {
      release();
    }
    this.releases = [];
  }
}

function batcher(client: FakeClient, maxConcurrentRequests = 4) {
  return new LiveStateBatcher({ client, batchDelayMs: 5, maxConcurrentRequests });
}

describe("LiveStateBatcher", () => {
  it("sends loads requested within the batch delay as one request, once per auction", async () => {
    const client = new FakeClient();
    client.missing.add("3");
    const loader = batcher(client);

    const lookups = await Promise.all([
      loader.load("1"),
      loader.load("2"),
      loader.load("1"),
      loader.load("3"),
    ]);

    expect(client.requests).toEqual([["1", "2", "3"]]);
    expect(lookups.map((lookup) => lookup.kind)).toEqual(["found", "found", "found", "missing"]);
    expect(lookups[0]).toBe(lookups[2]);
  });

  it("starts a new request for a load requested after its batch was sent", async () => {
    const client = new FakeClient();
    client.holdRequests = true;
    const loader = batcher(client);

    const first = loader.load("1");
    await waitFor(() => client.requests.length === 1, "the first request", 1_000);
    const second = loader.load("1");
    await waitFor(() => client.requests.length === 2, "the second request", 1_000);
    client.releaseAll();
    await Promise.all([first, second]);

    expect(client.requests).toEqual([["1"], ["1"]]);
  });

  it("splits a large batch into requests of 100 ids and bounds those in flight", async () => {
    const client = new FakeClient();
    client.holdRequests = true;
    const loader = batcher(client, 2);

    const loads = Array.from({ length: 250 }, (_, index) => loader.load(String(index + 1)));
    await waitFor(() => client.requests.length === 2, "two requests", 1_000);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(client.requests).toHaveLength(2);
    client.releaseAll();
    await waitFor(() => client.requests.length === 3, "the third request", 1_000);
    client.releaseAll();
    await Promise.all(loads);

    expect(client.requests.map((ids) => ids.length)).toEqual([100, 100, 50]);
  });

  it("rejects the loads of a failed request and of ids the response omitted", async () => {
    const client = new FakeClient();
    client.omitted.add("2");
    const loader = batcher(client);

    const results = await Promise.allSettled([loader.load("1"), loader.load("2")]);

    expect(results.map((result) => result.status)).toEqual(["fulfilled", "rejected"]);
    expect((results[1] as PromiseRejectedResult).reason).toEqual(
      new Error("live-state response omitted auction 2"),
    );
  });
});
