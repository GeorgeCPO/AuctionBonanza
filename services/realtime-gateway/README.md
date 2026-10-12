# Realtime Gateway

Streams committed auction changes to clients. It must not decide whether a bid
is valid or become the source of auction truth: everything it shows can be
rebuilt from `auction-service`.

The design is in [`docs/REALTIME_GATEWAY.md`](../../docs/REALTIME_GATEWAY.md):
a **relay** tier that consumes `auction.events` and scales with partitions,
and an **edge** tier that serves Server-Sent Events and scales with
connections, connected by Valkey sharded pub/sub.

Implemented so far:

- `src/relay.main.ts`, the relay process (`start:relay`, or
  `start:relay:local` with `.env.relay`; see `.env.relay.example`). It forwards
  each event to its auction's channel, `auction:{auctionId}`, commits offsets
  only after publishing, skips records it cannot forward, and pauses a
  partition while its publishes fail. A new group starts at the oldest
  retained event, so no event is skipped.
- `src/edge.main.ts`, the SSE edge process (`start:edge`, or
  `start:edge:local` with `.env.edge`; see `.env.edge.example`). It serves
  `GET /v1/stream?auctions=1,2,3` with one `snapshot` per auction (or
  `unavailable`), then `bid-accepted` events and a heartbeat comment every
  15 s. Auctions are
  shared by every client of the process: subscribed on first use, read from
  `auction-service` in batches, kept in contiguous version order, resynced on
  a gap or an interrupted subscription (a lost connection, a moved slot), and
  unsubscribed 30 s after their last client leaves. Its known gaps (slot moves
  repaired up to one check interval late, one unavailable shard stalling the
  whole edge, failover not yet exercised, no conflation) are listed under "Edge implementation
  status" in the design.
- `src/bus/`, the bus contract (channel names, envelopes, and their parsing),
  the Valkey publisher, and the Valkey subscriber (`valkey-bus-subscriber.ts`), which
  adds a periodic `CLUSTER SHARDS` check (`slot-map.ts`) to node-redis's
  cluster client.

`src/index.ts` is still a placeholder.

In `src/edge/`, `WatchedAuction` (`watched-auction.ts`) is one auction being
served: its clients, timers, and cached subscription, plus an `AuctionSync`
(`auction-sync.ts`) that owns its state and decides what to do with each
message, returning the effects as data. `auction-registry.ts` keeps the map of
auctions and does the work that needs the bus and auction-service.

In `src/bus/` and `src/edge/`, a module's types live next to it in
`<module>.types.ts` (for example `auction-registry.types.ts`). A type derived
from a TypeBox schema, such as `EdgeConfig` or `AuctionLiveState`, stays in the
file that defines the schema.

Integration tests need Kafka and the two Valkey clusters from the root
`docker-compose.yml`: the shared one, and the topology cluster that only tests
which move slots or fail over primaries use. Live states come from a stub of `auction-service`; the
path through the real one is covered by the root end-to-end suite,
`tests/e2e`.

```sh
TEST_KAFKA_BROKERS=localhost:9092 \
TEST_VALKEY_NODES=localhost:7001,localhost:7002,localhost:7003 \
TEST_VALKEY_TOPOLOGY_NODES=localhost:7101,localhost:7102,localhost:7103 \
pnpm --filter @auction-bonanza/realtime-gateway test
```
