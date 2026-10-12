import { pino } from "pino";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { auctionChannel, serializeAuctionEvent } from "../src/bus/bus.js";
import type { BusSubscriber } from "../src/bus/bus.types.js";
import { AuctionRegistry } from "../src/edge/auction-registry.js";
import type { AuctionRegistryStats, AuctionWatcher } from "../src/edge/auction-registry.types.js";
import type { LiveStateLookup, LiveStateSource } from "../src/edge/live-states.types.js";
import { bidAcceptedEvent, liveState } from "./support/edge-harness.js";

// A randomized check of what the whole design promises, run against the real
// AuctionRegistry. Each run builds a small world: a database whose auction
// gets bids, a bus that delivers events late, twice, out of order, or not at
// all, live-state reads that are slow or fail, connections that drop and come
// back, subscriptions that are lost, and clients that come and go. It then
// checks, for every client at every step:
//
//   1. its first message is a snapshot (or "unavailable"), never a change;
//   2. a change is always exactly one version after what it already has;
//   3. its version never goes backwards;
//
// and, once the world is quiet again, that every client holds the database's
// state. The world only loses events the way the real system does: while a
// connection is down, or when a subscription is dropped. Anything else the
// registry must handle (duplicates, reordering, late copies, slow reads).
//
// Runs are seeded. To replay one: SIM_SEED=123 npx vitest run <this file>.

const AUCTION = "42";
const RUNS = 250;
const STEPS_PER_RUN = 70;
// The last steps of a world commit nothing: an auction that goes quiet must
// repair itself, because no later event will reveal what it missed.
const QUIET_TAIL_STEPS = 20;
// Every this many steps the world goes quiet and every client is compared
// with the database, then the chaos resumes. A loss is usually repaired by the
// next event, so checking only once at the end would miss most of them.
const CHECKPOINT_EVERY = 14;

class Random {
  private state: number;
  constructor(seed: number) {
    this.state = seed >>> 0;
  }
  // mulberry32: small, fast, and good enough to drive a simulation.
  next(): number {
    this.state = (this.state + 0x6d2b79f5) | 0;
    let t = Math.imul(this.state ^ (this.state >>> 15), 1 | this.state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }
  int(maxExclusive: number): number {
    return Math.floor(this.next() * maxExclusive);
  }
  chance(probability: number): boolean {
    return this.next() < probability;
  }
  between(min: number, max: number): number {
    return min + this.int(max - min + 1);
  }
}

class SimBus implements BusSubscriber {
  readonly listeners = new Map<string, (message: string) => void>();
  private interruptionListener: () => void = () => {};
  private subscriptionLostListener: (channel: string) => void = () => {};
  private restored: PromiseWithResolvers<void> | undefined;

  async subscribe(channel: string, listener: (message: string) => void): Promise<void> {
    this.listeners.set(channel, listener);
  }
  async unsubscribe(channel: string): Promise<void> {
    this.listeners.delete(channel);
  }
  onInterruption(listener: () => void): void {
    this.interruptionListener = listener;
  }
  onSubscriptionLost(listener: (channel: string) => void): void {
    this.subscriptionLostListener = listener;
  }
  whenRestored(): Promise<void> {
    return this.restored?.promise ?? Promise.resolve();
  }

  loseConnection(): void {
    this.restored ??= Promise.withResolvers();
    this.interruptionListener();
  }
  restoreConnection(): void {
    this.restored?.resolve();
    this.restored = undefined;
  }
  loseSubscription(channel: string): void {
    this.listeners.delete(channel);
    this.subscriptionLostListener(channel);
  }
  deliver(channel: string, message: string): void {
    this.listeners.get(channel)?.(message);
  }
}

interface PendingRead {
  capturedAtRequest: LiveStateLookup;
  resolve: (lookup: LiveStateLookup) => void;
  reject: (error: unknown) => void;
}

class SimLiveStates implements LiveStateSource {
  readonly pending: PendingRead[] = [];
  constructor(private readonly database: () => LiveStateLookup) {}

  load(): Promise<LiveStateLookup> {
    const { promise, resolve, reject } = Promise.withResolvers<LiveStateLookup>();
    this.pending.push({ capturedAtRequest: this.database(), resolve, reject });
    return promise;
  }
}

type ClientView =
  | { kind: "nothing" }
  | { kind: "missing" }
  | { kind: "found"; version: number; leadingBid: { bidId: string; amountMinor: string } | null };

// What a browser applying the documented client rule would hold.
class SimClient implements AuctionWatcher {
  view: ClientView = { kind: "nothing" };
  readonly violations: string[] = [];

  constructor(readonly name: string) {}

  send(frame: string): void {
    const event = /^event: (.+)$/m.exec(frame)?.[1];
    const dataLine = /^data: (.+)$/m.exec(frame)?.[1];
    if (event === undefined || dataLine === undefined) return; // heartbeat
    const data = JSON.parse(dataLine) as {
      auctionVersion?: string;
      leadingBid?: { bidId: string; amountMinor: string } | null;
      bidId?: string;
      amountMinor?: string;
    };
    const version = Number(data.auctionVersion);

    if (event === "snapshot") {
      if (this.view.kind === "found" && version < this.view.version) {
        this.violations.push(`${this.name}: snapshot@${version} after v${this.view.version}`);
      }
      this.view = { kind: "found", version, leadingBid: data.leadingBid ?? null };
    } else if (event === "unavailable") {
      if (this.view.kind === "found") {
        this.violations.push(`${this.name}: unavailable after v${this.view.version}`);
      }
      this.view = { kind: "missing" };
    } else if (event === "bid-accepted") {
      if (this.view.kind !== "found") {
        this.violations.push(`${this.name}: bid-accepted@${version} before any snapshot`);
      } else if (version !== this.view.version + 1) {
        this.violations.push(
          `${this.name}: bid-accepted@${version} after v${this.view.version} (not contiguous)`,
        );
      }
      this.view = {
        kind: "found",
        version,
        leadingBid: { bidId: data.bidId!, amountMinor: data.amountMinor! },
      };
    }
  }
}

class Simulation {
  readonly rng: Random;
  readonly trace: string[] = [];
  private readonly bus = new SimBus();
  private readonly liveStates = new SimLiveStates(() => this.databaseLookup());
  private readonly registry: AuctionRegistry;
  private readonly channel = auctionChannel(AUCTION);
  private readonly clients: SimClient[] = [];
  private readonly everJoined: SimClient[] = [];

  // The database: the auction, and every event ever published for it.
  private exists: boolean;
  private version = 1;
  private readonly published: string[] = [];
  // Messages the bus accepted and has not delivered yet.
  private inflight: string[] = [];
  private connectionUp = true;

  constructor(readonly seed: number) {
    this.rng = new Random(seed);
    this.exists = this.rng.chance(0.8);
    this.registry = new AuctionRegistry({
      bus: this.bus,
      liveStates: this.liveStates,
      lingerMs: 150,
      maxBufferedEvents: this.rng.between(3, 8),
      retryBackoff: { baseDelayMs: 10, maxDelayMs: 40 },
      logger: pino({ level: "silent" }),
    });
  }

  async run(steps: number): Promise<AuctionRegistryStats> {
    this.join();
    await this.flush();
    for (let step = 0; step < steps; step += 1) {
      await this.randomStep(step >= steps - QUIET_TAIL_STEPS);
      await this.flush();
      this.assertNoViolations();
      if ((step + 1) % CHECKPOINT_EVERY === 0) await this.checkpoint();
    }
    await this.checkpoint();
    return this.registry.stats();
  }

  // Lets the world go quiet and checks that every client caught up. A client
  // arriving now gets whatever the edge holds, so one always joins.
  private async checkpoint(): Promise<void> {
    await this.settle();
    this.join();
    await this.settle();
    this.assertNoViolations();
    this.assertClientsMatchDatabase();
  }

  private async randomStep(quiet: boolean): Promise<void> {
    const { rng } = this;
    // Rolls below 26 are commits and creation; skip them when quiet.
    const roll = quiet ? 26 + rng.int(74) : rng.int(100);
    if (roll < 22) return this.commit();
    if (roll < 26) return this.create();
    if (roll < 44) return this.deliverInflight();
    if (roll < 49) return this.redeliverOldEvent();
    if (roll < 62) return this.answerRead();
    if (roll < 67) return this.interrupt();
    if (roll < 76) return this.restore();
    if (roll < 78) return this.loseSubscription();
    if (roll < 90) return this.tick();
    if (roll < 95) return this.join();
    return this.leave();
  }

  private commit(): void {
    if (!this.exists) return;
    this.version += 1;
    const message = serializeAuctionEvent(
      "BidAccepted.v1",
      bidAcceptedEvent(AUCTION, this.version),
    );
    this.published.push(message);
    // A connection that is down accepts nothing.
    if (this.connectionUp) this.inflight.push(message);
    this.log(`commit v${this.version}${this.connectionUp ? "" : " (lost: connection down)"}`);
  }

  // Creation is version 1 and emits no event, so an edge that holds "missing"
  // cannot learn the auction now exists until a bid arrives. That is a known
  // gap of the design (see "Edge implementation status" in
  // docs/REALTIME_GATEWAY.md), so the world creates the auction together with
  // its first bid, which is the part of the lifecycle the edge does cover.
  private create(): void {
    if (this.exists) return;
    this.exists = true;
    this.log("create auction (v1, no event)");
    this.commit();
  }

  private deliverInflight(): void {
    if (this.inflight.length === 0) return;
    const index = this.rng.int(this.inflight.length); // any order
    const message = this.inflight[index]!;
    // Sometimes the bus keeps a copy, so the same event arrives again later.
    if (!this.rng.chance(0.2)) this.inflight.splice(index, 1);
    this.log(`deliver ${versionOf(message)}`);
    this.bus.deliver(this.channel, message);
  }

  private redeliverOldEvent(): void {
    if (this.published.length === 0 || !this.connectionUp) return;
    const message = this.published[this.rng.int(this.published.length)]!;
    this.log(`late copy of ${versionOf(message)}`);
    this.bus.deliver(this.channel, message);
  }

  private answerRead(): void {
    if (this.liveStates.pending.length === 0) return;
    const [read] = this.liveStates.pending.splice(this.rng.int(this.liveStates.pending.length), 1);
    if (this.rng.chance(0.15)) {
      this.log("read fails");
      read!.reject(new Error("auction-service unavailable"));
      return;
    }
    // A read sees the database at some moment between its request and its
    // answer. Either end is allowed.
    const lookup = this.rng.chance(0.5) ? read!.capturedAtRequest : this.databaseLookup();
    this.log(`read answers ${lookupVersion(lookup)}`);
    read!.resolve(lookup);
  }

  private interrupt(): void {
    if (!this.connectionUp) return;
    this.connectionUp = false;
    this.inflight = []; // what the connection had not delivered is gone
    this.log("connection lost (in-flight events lost)");
    this.bus.loseConnection();
  }

  private restore(): void {
    if (this.connectionUp) return;
    this.connectionUp = true;
    this.log("connection restored");
    this.bus.restoreConnection();
  }

  private loseSubscription(): void {
    if (!this.connectionUp || !this.bus.listeners.has(this.channel)) return;
    this.inflight = []; // nothing listens until the edge subscribes again
    this.log("subscription dropped (in-flight events lost)");
    this.bus.loseSubscription(this.channel);
  }

  private async tick(): Promise<void> {
    const ms = this.rng.between(1, 300);
    this.log(`wait ${ms}ms`);
    await vi.advanceTimersByTimeAsync(ms);
  }

  private join(): void {
    if (this.clients.length >= 4) return;
    const client = new SimClient(`client${this.everJoined.length + 1}`);
    this.clients.push(client);
    this.everJoined.push(client);
    this.log(`${client.name} joins`);
    this.registry.watch(AUCTION, client);
  }

  private leave(): void {
    if (this.clients.length === 0) return;
    const [client] = this.clients.splice(this.rng.int(this.clients.length), 1);
    this.log(`${client!.name} leaves`);
    this.registry.unwatch(AUCTION, client!);
  }

  // Lets promise callbacks run, and fires timers that are due.
  private async flush(): Promise<void> {
    await vi.advanceTimersByTimeAsync(0);
  }

  // Ends the chaos and lets the edge catch up: connections come back, every
  // event and read gets through, time passes for retries.
  private async settle(): Promise<void> {
    this.log("-- settle --");
    for (let round = 0; round < 100; round += 1) {
      this.restore();
      while (this.inflight.length > 0) {
        this.bus.deliver(this.channel, this.inflight.shift()!);
        await this.flush();
      }
      while (this.liveStates.pending.length > 0) {
        // A read may still return the state as of its request.
        const read = this.liveStates.pending.shift()!;
        read.resolve(this.rng.chance(0.5) ? read.capturedAtRequest : this.databaseLookup());
        await this.flush();
      }
      await vi.advanceTimersByTimeAsync(100);
      const quiet =
        this.registry.stats().syncing === 0 &&
        this.liveStates.pending.length === 0 &&
        this.inflight.length === 0;
      if (quiet) return;
    }
    this.fail("the edge never finished syncing after the world went quiet");
  }

  private databaseLookup(): LiveStateLookup {
    return this.exists
      ? { kind: "found", state: liveState(AUCTION, this.version) }
      : { kind: "missing" };
  }

  private assertNoViolations(): void {
    const violations = this.everJoined.flatMap((client) => client.violations);
    if (violations.length > 0) this.fail(`a client saw an inconsistent stream: ${violations[0]}`);
  }

  private assertClientsMatchDatabase(): void {
    const expected: ClientView = this.exists
      ? {
          kind: "found",
          version: this.version,
          leadingBid:
            this.version === 1
              ? null
              : { bidId: String(1000 + this.version), amountMinor: String(100 * this.version) },
        }
      : { kind: "missing" };
    for (const client of this.clients) {
      if (JSON.stringify(client.view) !== JSON.stringify(expected)) {
        this.fail(
          `${client.name} ended at ${JSON.stringify(client.view)}, ` +
            `but the database is at ${JSON.stringify(expected)}`,
        );
      }
    }
  }

  private fail(reason: string): never {
    const recent = this.trace.slice(-45).join("\n  ");
    throw new Error(
      `${reason}\nseed ${this.seed} (replay: SIM_SEED=${this.seed} npx vitest run test/auction-registry.simulation.test.ts)\nlast steps:\n  ${recent}`,
    );
  }

  private log(line: string): void {
    this.trace.push(line);
  }
}

function versionOf(message: string): string {
  const envelope = JSON.parse(message) as { event: { auctionVersion: string } };
  return `v${envelope.event.auctionVersion}`;
}

function lookupVersion(lookup: LiveStateLookup): string {
  return lookup.kind === "found" ? `v${lookup.state.auctionVersion}` : "missing";
}

describe("registry simulation", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it(
    `keeps every client consistent across ${RUNS} random worlds`,
    { timeout: 120_000 },
    async () => {
      const only = process.env.SIM_SEED;
      const seeds =
        only === undefined ? Array.from({ length: RUNS }, (_, index) => index + 1) : [Number(only)];
      const total = {
        applied: 0,
        dropped: 0,
        buffered: 0,
        gap: 0,
        interrupted: 0,
        lost: 0,
        overflow: 0,
      };
      for (const seed of seeds) {
        const stats = await new Simulation(seed).run(STEPS_PER_RUN);
        total.applied += stats.eventsApplied;
        total.dropped += stats.eventsDropped;
        total.buffered += stats.eventsBuffered;
        total.gap += stats.resyncs.gap;
        total.interrupted += stats.resyncs["bus-interrupted"];
        total.lost += stats.resyncs["subscription-lost"];
        total.overflow += stats.resyncs["buffer-overflow"];
      }
      // A simulation that never reaches a situation proves nothing about it.
      // This holds for the default seeds; a single replayed seed may not.
      const unreached =
        only === undefined
          ? Object.entries(total)
              .filter(([, count]) => count === 0)
              .map(([what]) => what)
          : [];
      expect(unreached, "situations the simulation never reached").toEqual([]);
    },
  );
});
