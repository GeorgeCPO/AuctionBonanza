# End-to-end tests

Follows behavior across services, with every service a black box. Each test
compiles the services it needs from source into a throwaway directory, runs
each entry point as its own process, and talks to them only over HTTP, the
database (to create fixtures such as users), and the infrastructure from the
root `docker-compose.yml`. Nothing here imports a service's code.

- `test/realtime-bid.test.ts`: a bid placed over HTTP reaches an SSE client,
  through the auction-service API, its outbox publisher, Kafka, the relay,
  Valkey, and the edge. identity-service is stood in for by a test issuer that
  signs the access tokens auction-service verifies.

- The same file also covers the close: the finalizer process closes an auction
  whose deadline has passed, and the SSE client sees the closed auction with
  its winner.

Each run uses its own database (created from `db/schema.sql`), Kafka topic and
consumer group, and auction ids far from other tests' Valkey channels.

```sh
pnpm test:e2e:local
```

`pnpm test:e2e` is the same run without the local defaults: it needs
`TEST_DATABASE_URL`, `TEST_KAFKA_BROKERS` and `TEST_VALKEY_NODES` in the
environment and fails at startup, naming any that is missing. `:local` sets them
from `scripts/local-test.env`, which points at the root `docker-compose.yml`. See
[`docs/TESTING.md`](../../docs/TESTING.md) for why this tier is not measured for
coverage.
