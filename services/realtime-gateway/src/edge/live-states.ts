import { Type, type Static } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

import type {
  LiveStateBatcherOptions,
  LiveStateClientOptions,
  LiveStateFetcher,
  LiveStateLookup,
  LiveStateSource,
  PendingLoad,
} from "./live-states.types.js";

// The most ids auction-service accepts in one live-state request.
export const MAX_LIVE_STATE_IDS_PER_REQUEST = 100;

const timestamp = Type.String({ minLength: 1, maxLength: 40 });
const decimalId = Type.String({ pattern: "^[1-9][0-9]{0,18}$" });

const liveStateSchema = Type.Object({
  auctionId: decimalId,
  auctionVersion: decimalId,
  currencyCode: Type.String({ pattern: "^[A-Z]{3}$" }),
  startsAt: timestamp,
  endsAt: timestamp,
  cancelledAt: Type.Union([timestamp, Type.Null()]),
  closedAt: Type.Union([timestamp, Type.Null()]),
  phase: Type.String({ minLength: 1, maxLength: 64 }),
  leadingBid: Type.Union([
    Type.Object({ bidId: decimalId, amountMinor: Type.String({ pattern: "^[0-9]{1,19}$" }) }),
    Type.Null(),
  ]),
});

const liveStatesResponseSchema = Type.Object({
  items: Type.Array(liveStateSchema),
  missingIds: Type.Array(decimalId),
  observedAt: timestamp,
});

// One auction as GET /v1/auctions/live-states returns it: the version and
// the leading bid come from the same database snapshot. Values are kept as
// the strings auction-service sent; the edge forwards them unchanged.
export type AuctionLiveState = Static<typeof liveStateSchema>;

// Reads live states from auction-service in bulk.
export class LiveStateClient implements LiveStateFetcher {
  constructor(private readonly options: LiveStateClientOptions) {}

  async fetch(auctionIds: readonly string[]): Promise<Map<string, LiveStateLookup>> {
    const url = new URL("/v1/auctions/live-states", this.options.auctionServiceUrl);
    url.searchParams.set("ids", auctionIds.join(","));
    const response = await fetch(url, {
      headers: { accept: "application/json" },
      signal: AbortSignal.timeout(this.options.timeoutMs),
    });
    if (!response.ok) {
      throw new Error(`live-state request failed with HTTP ${response.status}`);
    }
    const body: unknown = await response.json();
    if (!Value.Check(liveStatesResponseSchema, body)) {
      throw new Error("live-state response does not match the expected shape");
    }

    const lookups = new Map<string, LiveStateLookup>();
    for (const state of body.items) lookups.set(state.auctionId, { kind: "found", state });
    for (const id of body.missingIds) lookups.set(id, { kind: "missing" });
    return lookups;
  }
}

// Collects live-state loads for a few milliseconds and sends them as bulk
// requests of up to 100 ids. Loads of the same auction that are still
// waiting share one entry. A load requested after its batch was sent starts
// a new one: a resync needs a read that begins after it was requested.
export class LiveStateBatcher implements LiveStateSource {
  private pending = new Map<string, PendingLoad>();
  private flushTimer: NodeJS.Timeout | undefined;
  private readonly queuedRequests: Map<string, PendingLoad>[] = [];
  private requestsInFlight = 0;

  constructor(private readonly options: LiveStateBatcherOptions) {}

  load(auctionId: string): Promise<LiveStateLookup> {
    let load = this.pending.get(auctionId);
    if (load === undefined) {
      const { promise, resolve, reject } = Promise.withResolvers<LiveStateLookup>();
      load = { promise, resolve, reject };
      this.pending.set(auctionId, load);
    }
    this.flushTimer ??= setTimeout(() => this.flush(), this.options.batchDelayMs);
    return load.promise;
  }

  private flush(): void {
    this.flushTimer = undefined;
    const batch = [...this.pending];
    this.pending = new Map();
    for (let start = 0; start < batch.length; start += MAX_LIVE_STATE_IDS_PER_REQUEST) {
      this.queuedRequests.push(new Map(batch.slice(start, start + MAX_LIVE_STATE_IDS_PER_REQUEST)));
    }
    this.sendQueuedRequests();
  }

  private sendQueuedRequests(): void {
    while (this.requestsInFlight < this.options.maxConcurrentRequests) {
      const request = this.queuedRequests.shift();
      if (request === undefined) return;
      this.requestsInFlight += 1;
      void this.send(request).finally(() => {
        this.requestsInFlight -= 1;
        this.sendQueuedRequests();
      });
    }
  }

  private async send(request: Map<string, PendingLoad>): Promise<void> {
    try {
      const lookups = await this.options.client.fetch([...request.keys()]);
      for (const [auctionId, load] of request) {
        const lookup = lookups.get(auctionId);
        if (lookup === undefined) {
          load.reject(new Error(`live-state response omitted auction ${auctionId}`));
        } else {
          load.resolve(lookup);
        }
      }
    } catch (error) {
      for (const load of request.values()) load.reject(error);
    }
  }
}
