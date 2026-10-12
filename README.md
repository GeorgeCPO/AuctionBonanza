# Auction Bonanza

Real-time auction platform

Users can create auctions, place bids, watch prices update live, use automatic/max bidding, and eventually settle an auction into an order/payment.

Then add load-generating bots so you're dealing with millions of auctions and hundreds of millions of bids rather than your own five test users.

## Description

Develop a robust auction backend that will confront "distributed system" problems.

This is a project aimed to turn a competent backend engineer into someone who can confidently discuss Kafka partition strategy, database internals, distributed consistency, SQL optimization, migrations and AWS architecture in a senior-level interview.

## Tech stack

- Node.js
- TypeScript
- PostgreSQL
- Kafka
- AWS stack
- Open for extension

## Local foundation

Prerequisites are Node.js 24.20.0, pnpm 11.25.0, Docker with Compose, and
PostgreSQL client tools containing `pg_dump` 18.x for schema snapshots (or use
the container's `pg_dump`, see [Continuous integration](#continuous-integration)).

Direct dependencies are pinned to exact versions. Use `pnpm add --save-exact`
when adding a dependency (the repository enforces this default), and commit the
resulting `pnpm-lock.yaml` change.

```sh
pnpm install
pnpm check
docker compose up -d postgres kafka kafka-init valkey-init valkey-topology-init
cp .env.example .env
pnpm db:migrate
pnpm auth:keys:local local-2026-09
```

Use `pnpm format` to apply repository formatting, `pnpm format:check` to verify
it without writing files, and `pnpm lint` for type-aware linting. `pnpm check`
runs formatting, linting, type checks, unit tests with their coverage
thresholds, and production builds; it needs no infrastructure. `pnpm test` runs
only the unit tests. Integration and end-to-end tests are separate commands,
described in [`docs/TESTING.md`](docs/TESTING.md).

The Compose stack contains PostgreSQL, a single-node Kafka, and a three-node
Valkey cluster. PostgreSQL runs
`postgres:18.6-bookworm` to match the planned Amazon RDS for PostgreSQL 18.6
engine version, and exposes the local `auction_db` database on port `5432`
using the development-only credentials in `docker-compose.yml`.

Kafka runs `apache/kafka:4.3.1` in KRaft mode (combined broker and controller,
no ZooKeeper) with data in the `kafka43_data` volume. Host processes connect to
`localhost:9092`; containers use `kafka:29092`. Its health check waits until
the broker answers API requests. Automatic topic creation is disabled, so the
one-shot `kafka-init` service provisions topics explicitly once Kafka is
healthy:

| Topic            | Partitions | Key         | Retention |
| ---------------- | ---------- | ----------- | --------- |
| `auction.events` | 12         | `auctionId` | 7 days    |

`kafka-init` is idempotent (`--if-not-exists`) and exits after printing the
topic description; rerun it with `docker compose up kafka-init`. It never
changes the partition count of an existing topic: adding partitions remaps keys
and would break per-auction ordering for in-flight data, so it must be a
deliberate operation. Inspect topics with:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic auction.events
```

The broker also exposes a test-only `FAULT_INJECTION` listener on
`localhost:19093` that advertises `localhost:19092`, where the Kafka outage
integration test runs a TCP proxy. Nothing else should use it.

Valkey runs `valkey/valkey:9.0.6`, matching Amazon ElastiCache for Valkey 9.0,
as the realtime gateway's pub/sub bus (see
[`docs/REALTIME_GATEWAY.md`](docs/REALTIME_GATEWAY.md)). It runs in cluster
mode with three primaries and no replicas on `localhost:7001`–`7003`. Each node
announces the hostname `localhost`, so host processes can follow the endpoints
in `CLUSTER SHARDS` replies and `MOVED` redirects; containers cannot use them
yet. There is no persistence: the bus carries only pub/sub traffic, and cluster
membership lives in memory. The one-shot `valkey-init` service forms the
cluster once all three nodes are healthy. It changes nothing when the nodes
already form one cluster covering every slot. Otherwise, for example after a
node restarted blank, it resets all three and forms the cluster again. Rerun it
with `docker compose up valkey-init`, and inspect the cluster with:

```sh
docker compose exec valkey-1 valkey-cli -p 7001 cluster shards
```

A second cluster, `valkey-topology-1`–`6` on `localhost:7101`–`7106` and formed
by `valkey-topology-init`, is for tests that change the topology, such as
moving a hash slot to another primary. Nothing else connects to it, so what
those tests do cannot break a test that shares the first cluster. It has the
same image and settings: three primaries (7101–7103) and one replica each
(7104–7106), so tests can also fail a primary over.

Tests come in three tiers, each in its own folder with its own command (see
[`docs/TESTING.md`](docs/TESTING.md) for the rules and for coverage):

| Command                 | Runs                                                      | Needs the Compose stack |
| ----------------------- | --------------------------------------------------------- | ----------------------- |
| `pnpm test:unit`        | `test/unit/` in every workspace                           | No                      |
| `pnpm test:integration` | `test/integration/`, against real PostgreSQL/Kafka/Valkey | Yes                     |
| `pnpm test:e2e`         | `tests/e2e`, every service as a black box                 | Yes                     |

Integration and end-to-end tests do not skip themselves: they refuse to start
unless the `TEST_*` variables they need are set, and say which are missing.
`pnpm test:integration:local` and `pnpm test:e2e:local` set them for you from
`scripts/local-test.env`, which points at the Compose stack above:

```sh
pnpm test:integration:local
pnpm test:e2e:local
```

`tests/e2e` is the end-to-end suite. It compiles the services from source and
runs each as its own process (the auction-service API, outbox publisher and
finalizer, the relay, and the edge), then follows a bid from HTTP to an SSE
client. It treats every service as a black box, so each service's own tests stay
self-contained.

The outbox publisher tests, the finalizer tests and the end-to-end suite create
and drop their own temporary databases from `db/schema.sql` (so the database user
needs `CREATEDB`), and their own Kafka topics. The Kafka outage test needs port
`19092` free and reaches the broker through `TEST_KAFKA_FAULT_INJECTION_UPSTREAM`.

## Continuous integration

GitHub Actions (`.github/workflows/ci.yml`) runs one job on every pull request
and on pushes to `master`:

1. Starts `postgres`, `kafka`, and the six Valkey nodes (the shared cluster
   and the topology cluster) from the root `docker-compose.yml`, waits for
   their health checks, and requires the one-shot `kafka-init`, `valkey-init`
   and `valkey-topology-init` to exit with 0.
2. Installs with `pnpm install --frozen-lockfile` and checks that the native
   Kafka client (librdkafka) loads.
3. Runs `pnpm db:migrate`, which regenerates `db/schema.sql` with `pg_dump`
   from the postgres container, and fails if the snapshot differs from the
   committed file. Only the `-- Dumped from/by` version header lines are
   ignored, because they depend on the committer's `pg_dump` build.
4. Runs `pnpm check` (format, lint, typecheck, unit tests with coverage
   thresholds, build), then `pnpm test:integration`, then `pnpm test:e2e`, as
   separate steps so a failure names its tier. The `TEST_*` variables are set
   for the whole job, so every integration test runs, including the Kafka
   outage test.

The integration and end-to-end Vitest configurations fail before running any
test if a variable they need is missing, so neither tier can be skipped
silently.

To reproduce the CI run locally (port `19092` must be free):

```sh
docker compose up --detach --wait postgres kafka valkey-1 valkey-2 valkey-3 \
  valkey-topology-1 valkey-topology-2 valkey-topology-3 \
  valkey-topology-4 valkey-topology-5 valkey-topology-6
docker compose up kafka-init valkey-init valkey-topology-init
PATH="$PWD/scripts/compose-bin:$PATH" pnpm db:migrate
git diff --exit-code --ignore-matching-lines='^-- Dumped (from|by) ' -- db/schema.sql
pnpm check:all:local
```

`check:all:local` runs `check`, `test:integration` and `test:e2e` with `CI=true`
and the endpoints from `scripts/local-test.env`.

`scripts/compose-bin/pg_dump` runs `pg_dump` inside the postgres container, so
it also works locally without PostgreSQL client tools. CI starts from empty
volumes; a local stack keeps its data, so a clean run needs
`docker compose down --volumes` first, which deletes local data.

Database migrations are owned centrally because the initial services share
`auction_db`. Use `pnpm db:status`, `pnpm db:migrate`, and `pnpm db:rollback`
from the repository root. Migrate and rollback regenerate the committed
`db/schema.sql` snapshot. See `db/migrations/README.md` for the workflow, naming,
and schema decisions.

PostgreSQL 18 uses the versioned `postgres18_data` Compose volume mounted at
`/var/lib/postgresql`, matching the official image's persistent-storage layout.

To exercise the identity service's runtime configuration loader:

```sh
cp services/identity-service/.env.example services/identity-service/.env
pnpm --filter @auction-bonanza/identity-service build
pnpm --filter @auction-bonanza/identity-service start:local
```

The process validates all configuration before service startup. The `.env` file
is ignored by Git; `.env.example` is the committed local-development contract.
Signing keys under `services/identity-service/.local-keys` are also ignored and
are for local development only.

The auction service uses the identity service's public JWKS and the same access
token issuer/audience. To run it after applying migrations and starting
identity:

```sh
cp services/auction-service/.env.example services/auction-service/.env
pnpm --filter @auction-bonanza/auction-service build
pnpm --filter @auction-bonanza/auction-service start:local
```

Accepted bids reach Kafka through the auction service's separate outbox
publisher worker:

```sh
cp services/auction-service/.env.outbox-publisher.example services/auction-service/.env.outbox-publisher
pnpm --filter @auction-bonanza/auction-service build
pnpm --filter @auction-bonanza/auction-service start:outbox-publisher:local
```

Its delivery and ordering guarantees are described in
[`docs/OUTBOX_PUBLISHING.md`](docs/OUTBOX_PUBLISHING.md).

Auctions whose deadline has passed are closed by the finalizer, a third
`auction-service` process. It records the winning bid (or an unsold result) and
emits `AuctionClosed.v1` through the outbox, described in
[`docs/FINALIZATION.md`](docs/FINALIZATION.md):

```sh
cp services/auction-service/.env.finalizer.example services/auction-service/.env.finalizer
pnpm --filter @auction-bonanza/auction-service build
pnpm --filter @auction-bonanza/auction-service start:finalizer:local
```

The realtime gateway's relay forwards those events from Kafka to the Valkey
cluster, one sharded channel per auction:

```sh
cp services/realtime-gateway/.env.relay.example services/realtime-gateway/.env.relay
pnpm --filter @auction-bonanza/realtime-gateway build
pnpm --filter @auction-bonanza/realtime-gateway start:relay:local
```

Watch an auction's channel with
`docker compose exec valkey-1 valkey-cli -c -p 7001 ssubscribe auction:<id>`
(`-c` follows the redirect to the node that owns the channel). Its guarantees
are described in [`docs/REALTIME_GATEWAY.md`](docs/REALTIME_GATEWAY.md#relay).

The gateway's SSE edge serves those channels to clients on port 3003. It reads
live states from `auction-service`, so start the API first:

```sh
cp services/realtime-gateway/.env.edge.example services/realtime-gateway/.env.edge
pnpm --filter @auction-bonanza/realtime-gateway start:edge:local
curl -N 'http://localhost:3003/v1/stream?auctions=<id>'
```

Each stream starts with one `snapshot` per auction (or `unavailable` for an
auction that does not exist), then `bid-accepted` events; see
[`docs/REALTIME_GATEWAY.md`](docs/REALTIME_GATEWAY.md#client-protocol).
