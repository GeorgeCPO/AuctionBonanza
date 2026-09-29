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
docker compose up -d postgres kafka kafka-init
cp .env.example .env
pnpm db:migrate
pnpm auth:keys:local local-2026-09
```

Use `pnpm format` to apply repository formatting, `pnpm format:check` to verify
it without writing files, and `pnpm lint` for type-aware linting. `pnpm check`
runs formatting, linting, type checks, tests, and production builds.

The Compose stack contains PostgreSQL and a single-node Kafka. PostgreSQL runs
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

Integration tests are skipped unless their infrastructure is configured:

```sh
TEST_DATABASE_URL=postgresql://auction:auction_local@localhost:5432/auction_db \
TEST_KAFKA_BROKERS=localhost:9092 \
TEST_KAFKA_FAULT_INJECTION_UPSTREAM=localhost:19093 \
pnpm --filter @auction-bonanza/auction-service test
```

The outbox publisher tests create and drop their own temporary databases from
`db/schema.sql` (so the database user needs `CREATEDB`), and their own Kafka
topics. `TEST_KAFKA_FAULT_INJECTION_UPSTREAM` enables only the broker outage
test.

## Continuous integration

GitHub Actions (`.github/workflows/ci.yml`) runs one job on every pull request
and on pushes to `master`:

1. Starts `postgres` and `kafka` from the root `docker-compose.yml`, waits for
   their health checks, and requires the one-shot `kafka-init` to exit with 0.
2. Installs with `pnpm install --frozen-lockfile` and checks that the native
   Kafka client (librdkafka) loads.
3. Runs `pnpm db:migrate`, which regenerates `db/schema.sql` with `pg_dump`
   from the postgres container, and fails if the snapshot differs from the
   committed file. Only the `-- Dumped from/by` version header lines are
   ignored, because they depend on the committer's `pg_dump` build.
4. Runs `pnpm check` with all `TEST_*` variables set, so every integration
   test runs, including the Kafka outage test.

Integration tests still skip themselves locally when their variables are
unset. When `CI=true`, the auction and identity service Vitest configurations
instead fail before running any test if a variable they need is missing.

To reproduce the CI run locally (port `19092` must be free):

```sh
docker compose up --detach --wait postgres kafka
docker compose up kafka-init
PATH="$PWD/scripts/compose-bin:$PATH" pnpm db:migrate
git diff --exit-code --ignore-matching-lines='^-- Dumped (from|by) ' -- db/schema.sql
CI=true \
TEST_DATABASE_URL=postgresql://auction:auction_local@localhost:5432/auction_db \
TEST_KAFKA_BROKERS=localhost:9092 \
TEST_KAFKA_FAULT_INJECTION_UPSTREAM=localhost:19093 \
pnpm check
```

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
