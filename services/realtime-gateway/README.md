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
- `src/bus/`, the bus contract (channel names and envelopes) and the Valkey
  publisher.

The SSE edge is the next slice; `src/index.ts` is still a placeholder.

Integration tests need Kafka and the Valkey cluster from the root
`docker-compose.yml`:

```sh
TEST_KAFKA_BROKERS=localhost:9092 \
TEST_VALKEY_NODES=localhost:7001,localhost:7002,localhost:7003 \
pnpm --filter @auction-bonanza/realtime-gateway test
```
