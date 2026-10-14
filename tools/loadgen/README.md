# loadgen

A Go tool for the scale scenarios in [`ACCEPTANCE.md`](../../ACCEPTANCE.md). It is
not a service and shares no code with the TypeScript services. This first stage
has two jobs:

- `seed` puts users and auctions into a running stack and writes a manifest of
  what exists, including the Kafka partition each auction's events will use.
- `sample` polls PostgreSQL and Kafka once a second and writes CSV, so an
  experiment leaves numbers behind.

A bid generator, an SSE client swarm and a Kafka backlog producer come later and
reuse the packages here (`token`, `murmur2`, the manifest).

Why it is Go is explained in [`docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md).
The Go version is the `go` line in `go.mod`; dependencies are pinned by `go.mod`
and `go.sum`.

Run everything from the repository root. Defaults for key and output paths are
relative to it.

```sh
pnpm loadgen:build            # builds tools/loadgen/bin/loadgen
tools/loadgen/bin/loadgen help
```

## Seeding

Prerequisites: the Compose stack, `pnpm db:migrate`, `pnpm auth:keys:local`, and
identity-service and auction-service running (auction-service fetches identity's
JWKS to verify tokens).

```sh
tools/loadgen/bin/loadgen seed --users 100 --auctions 1000
```

| Flag                     | Default                                    | Meaning                                                           |
| ------------------------ | ------------------------------------------ | ----------------------------------------------------------------- |
| `--users`                | 100                                        | Seeded users, at least 2 so bidders can differ from owners        |
| `--auctions`             | 1000                                       | Auctions to create; owners rotate through the users               |
| `--concurrency`          | 16                                         | Requests in flight                                                |
| `--duration`             | 168h                                       | How long each auction stays open (2 minutes to 29 days)           |
| `--currency`             | USD                                        | Must be in auction-service's `SUPPORTED_CURRENCY_CODES`           |
| `--partitions`           | 0 (ask the brokers)                        | Partitions of `--topic`, used to compute each auction's partition |
| `--brokers`, `--topic`   | `KAFKA_BROKERS`, `AUCTION_EVENTS_TOPIC`    | Where `--partitions 0` looks the count up                         |
| `--out`                  | `tools/loadgen/out/manifest.json`          | Manifest path (compact JSON; read it with `jq`)                   |
| `--database-url`         | `DATABASE_URL`, the Compose database       | PostgreSQL                                                        |
| `--api-url`              | `AUCTION_API_URL`, `http://localhost:3002` | auction-service                                                   |
| `--key`, `--jwks`        | `services/identity-service/.local-keys/…`  | Signing material from `pnpm auth:keys:local`                      |
| `--issuer`, `--audience` | `AUTH_ISSUER`, `AUTH_AUDIENCE`             | Must match auction-service's `AUTH_ISSUER` and `AUTH_AUDIENCE`    |
| `--ttl`                  | 5m                                         | At most auction-service's `ACCESS_TOKEN_TTL_SECONDS`              |

Decisions worth knowing:

- **Users are inserted with SQL**, not registered through identity. Identity owns
  the `users` table and hashes passwords with argon2id on purpose, which makes
  registering thousands of users over HTTP slow. Seeded users are
  `loadgen-<n>@loadgen.invalid` with a password hash nothing can match, so they
  can only act through tokens this tool mints. This is a deliberate exception to
  table ownership. Rerunning reuses existing users.
- **Auctions go through the API**, so they pass auction-service's validation and
  ownership rules. Tokens are minted in process with the local signing key.
- **Creation has no idempotency key**, so only a failure to reach the server
  (connection refused) is retried. After any other failure the auction may exist;
  it is counted as failed, never sent twice. The command exits 1 if any failed.
- **The end time is computed when each request is sent**, so a long seeding run
  does not leave later auctions with an almost-passed deadline. The finalizer
  closes auctions after `ends_at`, hence the 7-day default for load runs.
- **Partitions are predicted, not chosen.** Auction ids are database-generated, so
  the tool cannot place an auction on a partition. It reads the partition count
  from the topic, so the manifest cannot disagree with it, computes the partition
  (Kafka's murmur2 of the decimal id) and records it. To get N auctions on one
  partition, seed about `partitions × N` and use those that map to it.

`tools/loadgen/bin/loadgen token --user <id>` prints a bearer token for trying
the API by hand.

## Sampling

```sh
tools/loadgen/bin/loadgen sample                       # until Ctrl-C
tools/loadgen/bin/loadgen sample --duration 60s --interval 500ms
```

It writes `samples.csv`, `partitions.csv` and `summary.txt` to
`tools/loadgen/out/samples-<time>/` (`--out-dir`) and prints the summary when it
stops. Rows are flushed as they are written, so `tail -f` follows a live run.
An empty cell means unknown: when PostgreSQL or Kafka cannot be read, the tick
still gets a row, so an outage shows up as a gap.

`samples.csv`, one row per tick:

| Column                            | Source                                                                      |
| --------------------------------- | --------------------------------------------------------------------------- |
| `bids_issued`, `bids_per_sec`     | `bids_id_seq`. An upper bound: ids burned by rolled-back transactions count |
| `orders_issued`, `orders_per_sec` | `orders_id_seq`, same caveat                                                |
| `outbox_unpublished`              | Rows in `outbox_events` with `published_at IS NULL`                         |
| `outbox_oldest_age_s`             | Age of the oldest unpublished event                                         |
| `outbox_retrying`                 | Unpublished events that already failed at least once                        |
| `lock_waiters`                    | Connections waiting on a lock: the signature of a hot auction row           |
| `idle_in_transaction`, `active`   | Connection states, excluding the sampler's own                              |
| `events_per_sec`                  | Records per second arriving on the topic, all partitions                    |
| `lag_<group>`                     | Records the group has yet to consume, summed over partitions                |

`partitions.csv`, one row per partition per tick: `start_offset`, `end_offset`,
`produce_per_sec`, and per group `committed_<group>` and `lag_<group>`.

How to read it:

- The sampler never counts the `bids` table, which at hundreds of millions of
  rows would be the load it is watching. Bid and order totals come from sequences,
  which is O(1). The outbox count reads a partial index holding only unpublished
  rows, so its cost grows with the backlog it reports. The database reading is one
  statement, so it costs one round trip.
- A tick waits at most `--read-timeout` (5 s by default) for PostgreSQL and Kafka,
  which is independent of `--interval`; the database's `statement_timeout` is set
  to the same value. A read that times out leaves a gap, and a slow tick delays the
  next one instead of overlapping it.
- A group that has never committed on the topic (or does not exist) has empty
  lag cells, not zero. In a group that has committed somewhere, a partition with
  no commit counts its whole contents as lag, which assumes the group starts at
  the earliest offset.
- Sampling only reads. It joins no consumer group, so it cannot trigger a
  rebalance.
- Rates divide a counter's change by the time since the last _successful_ reading
  of that source, so a rate after a gap is an average over the gap. A counter that
  went down (a reset database, a recreated topic) gives no rate for that tick.

## Checks

`pnpm loadgen:check` is the hermetic gate and `pnpm loadgen:test:integration:local`
runs the tests that need the Compose stack; both run in CI. The commands, the
`integration` build tag and the rule that a missing `TEST_*` variable fails the
run are in [`docs/TESTING.md`](../../docs/TESTING.md#the-go-load-tool).

## Verification record

- **murmur2 against the real publisher.** On 2026-10-14, one bid on an auction
  from each of the 12 partitions went through the real outbox publisher
  (librdkafka, `murmur2_random`); all 12 records landed on the partition the tool
  predicted. The integration test cannot show this: it compares against franz-go's
  partitioner, which is an independent implementation of Kafka's hash but not
  librdkafka. Repeat the manual check if the publisher's partitioner setting changes.
- **Lag against Kafka's own tool.** At rest, `kafka-consumer-groups.sh --describe`
  and the sampler agreed on zero lag for `settlement-worker`.

## First observation

A smoke run, not a measurement: on a local stack with default settings (one
outbox publisher, batch 50, 250 ms poll), 110 bids on 3 auctions that share a
partition were accepted in about a second, and the outbox then drained at a
steady 9–12 events per second, with the oldest unpublished event reaching 5 s old.
That rate is what the outbox design predicts for 3 hot auctions (a batch holds at
most one event per auction), so the publisher's per-auction ordering, not Kafka,
looks like the first limit a hot auction meets. Confirming it needs a proper
experiment with controlled publisher settings.
