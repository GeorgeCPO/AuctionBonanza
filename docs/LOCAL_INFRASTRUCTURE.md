# Local Infrastructure and CI

Reference for the Docker Compose stack, the database workflow, and the
continuous integration job. The step-by-step setup is in the
[README](../README.md#run-it-locally); this document explains what the pieces
are and why they are configured the way they are.

## Compose services

One root `docker-compose.yml` defines every dependency, locally and in CI.

| Service                 | Purpose                                               | Host port(s)     |
| ----------------------- | ----------------------------------------------------- | ---------------- |
| `postgres`              | `auction_db`                                          | `5432`           |
| `kafka`                 | Single-node Kafka in KRaft mode                       | `9092`           |
| `kafka-init`            | One-shot: creates the topics below                    | n/a              |
| `valkey-1`–`3`          | Three-primary Valkey cluster for the realtime bus     | `7001`–`7003`    |
| `valkey-init`           | One-shot: forms that cluster                          | n/a              |
| `valkey-topology-1`–`6` | Second cluster, only for topology-changing tests      | `7101`–`7106`    |
| `valkey-topology-init`  | One-shot: forms the second cluster                    | n/a              |
| `kafka-ui` (optional)   | Kafbat UI to browse topics, messages and consumer lag | `127.0.0.1:8080` |

Running the application needs only `postgres`, `kafka`, `kafka-init`, and the
first Valkey cluster. The topology cluster is for the integration tests.

### PostgreSQL

`postgres:18.6-bookworm` matches the planned Amazon RDS for PostgreSQL 18.6
engine version. It exposes `auction_db` on port `5432` using the
development-only credentials in `docker-compose.yml`. Data lives in the
versioned `postgres18_data` volume, mounted at `/var/lib/postgresql` as the
official 18 image expects.

### Kafka

Kafka runs `apache/kafka:4.3.1` in KRaft mode (combined broker and controller,
no ZooKeeper) with data in the `kafka43_data` volume. Host processes connect to
`localhost:9092`; containers use `kafka:29092`. Its health check waits until
the broker answers API requests. Automatic topic creation is disabled, so the
one-shot `kafka-init` service provisions topics explicitly once Kafka is
healthy:

| Topic                | Partitions | Key         | Retention |
| -------------------- | ---------- | ----------- | --------- |
| `auction.events`     | 12         | `auctionId` | 7 days    |
| `auction.events.dlq` | 1          | as received | 30 days   |

`auction.events.dlq` holds the records the settlement worker could never process
(see [`SETTLEMENT.md`](SETTLEMENT.md#the-dead-letter-topic)).

`kafka-init` is idempotent (`--if-not-exists`) and exits after printing the
topic description; rerun it with `docker compose up kafka-init`. It never
changes the partition count of an existing topic: adding partitions remaps keys
and would break per-auction ordering for in-flight data, so it must be a
deliberate operation. Inspect topics with:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic auction.events
```

To browse topics, messages, and consumer-group lag in a web UI, start Kafbat UI
and open <http://localhost:8080>:

```sh
docker compose up -d kafka-ui
```

It connects to `kafka:29092` like any other container client and is bound to
`127.0.0.1` because it has no login and can delete topics and reset offsets. It
is a local tool; a production deployment would need authentication and a
read-only configuration.

The broker also exposes a test-only `FAULT_INJECTION` listener on
`localhost:19093` that advertises `localhost:19092`, where the Kafka outage
integration test runs a TCP proxy. Nothing else should use it.

### Valkey

Valkey runs `valkey/valkey:9.0.6`, matching Amazon ElastiCache for Valkey 9.0,
as the realtime gateway's pub/sub bus (see
[`REALTIME_GATEWAY.md`](REALTIME_GATEWAY.md)). It runs in cluster mode with
three primaries and no replicas on `localhost:7001`–`7003`. Each node announces
the hostname `localhost`, so host processes can follow the endpoints in
`CLUSTER SHARDS` replies and `MOVED` redirects; containers cannot use them yet.
There is no persistence: the bus carries only pub/sub traffic, and cluster
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

Watch an auction's channel with
`docker compose exec valkey-1 valkey-cli -c -p 7001 ssubscribe auction:<id>`
(`-c` follows the redirect to the node that owns the channel).

## Database migrations

Migrations are owned centrally because the initial services share `auction_db`.
Use `pnpm db:status`, `pnpm db:migrate`, and `pnpm db:rollback` from the
repository root. Migrate and rollback regenerate the committed `db/schema.sql`
snapshot with `pg_dump`. See [`db/migrations/README.md`](../db/migrations/README.md)
for the workflow, naming, and schema decisions.

`scripts/compose-bin/pg_dump` runs `pg_dump` inside the postgres container, so
the commands also work without PostgreSQL client tools:

```sh
PATH="$PWD/scripts/compose-bin:$PATH" pnpm db:migrate
```

Without that prefix, `pnpm db:migrate` needs a local `pg_dump` 18.x.

## Tests that need the stack

Integration and end-to-end tests do not skip themselves: they refuse to start
unless the `TEST_*` variables they need are set, and say which are missing.
`pnpm test:integration:local` and `pnpm test:e2e:local` set them from
`scripts/local-test.env`, which points at the Compose stack. The tiers and their
rules are in [`TESTING.md`](TESTING.md).

The outbox publisher tests, the finalizer tests, the settlement tests and the
end-to-end suite create and drop their own temporary databases from
`db/schema.sql` (so the database user needs `CREATEDB`), and their own Kafka
topics. The Kafka outage test needs port `19092` free and reaches the broker
through `TEST_KAFKA_FAULT_INJECTION_UPSTREAM`.

`tests/e2e` compiles the services from source and runs each as its own process
(the auction-service API, outbox publisher and finalizer, the relay, the edge,
and the settlement worker), then follows a bid from HTTP to an SSE client, and
a closed auction to its order. It treats every service as a black box, so each
service's own tests stay self-contained.

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

CI starts from empty volumes; a local stack keeps its data, so a clean run needs
`docker compose down --volumes` first, which deletes local data.
