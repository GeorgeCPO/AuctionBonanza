# loadgen

A Go tool for the scale scenarios in the [roadmap](../../ACCEPTANCE.md). It is
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

| Flag                     | Default                                    | Meaning                                                                                                                       |
| ------------------------ | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `--users`                | 100                                        | Seeded users, at least 2 so bidders can differ from owners                                                                    |
| `--auctions`             | 1000                                       | Auctions to create; owners rotate through the users                                                                           |
| `--concurrency`          | 16                                         | Requests in flight                                                                                                            |
| `--abort-after`          | 10                                         | Stop after this many failures in a row; 0 never stops                                                                         |
| `--duration`             | 168h                                       | How long each auction stays open (2 minutes to 29 days)                                                                       |
| `--currency`             | USD                                        | Must be in auction-service's `SUPPORTED_CURRENCY_CODES`                                                                       |
| `--partitions`           | 0 (ask the brokers)                        | Partitions of `--topic`, used to compute each auction's partition                                                             |
| `--brokers`, `--topic`   | `KAFKA_BROKERS`, `AUCTION_EVENTS_TOPIC`    | Where `--partitions 0` looks the count up                                                                                     |
| `--out`                  | `tools/loadgen/out/manifest.json`          | Manifest path (compact JSON; read it with `jq`)                                                                               |
| `--database-url`         | `DATABASE_URL`, the Compose database       | PostgreSQL                                                                                                                    |
| `--api-url`              | `AUCTION_API_URL`, `http://localhost:3002` | auction-service                                                                                                               |
| `--key`, `--jwks`        | `services/identity-service/.local-keys/…`  | Signing material from `pnpm auth:keys:local`; the JWKS entry that matches the key is used, and a JWKS without one is an error |
| `--issuer`, `--audience` | `AUTH_ISSUER`, `AUTH_AUDIENCE`             | Must match auction-service's `AUTH_ISSUER` and `AUTH_AUDIENCE`                                                                |
| `--ttl`                  | 5m                                         | At most auction-service's `ACCESS_TOKEN_TTL_SECONDS`                                                                          |

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
  (connection refused) is retried, never anything that might have been processed.
  Every requested auction that is not in the manifest is counted as one of:
  - _rejected_: the server answered 4xx, so the auction does not exist;
  - _not sent_: the request never reached the server, or was never attempted
    (including everything after an abort or Ctrl-C), so it does not exist;
  - _unknown_: a 5xx, a timeout, a reset connection, or a request cut short by
    Ctrl-C. **These may exist but are not in the manifest**, and rerunning will not
    find them. In a run interrupted with Ctrl-C, all 31 requests that were in flight
    turned out to exist in the database.

  The counts always add up to the number requested, and the command exits 1 if any
  auction is missing.

- **A run that fails systematically stops itself.** After `--abort-after` failures
  in a row (a wrong `--audience`, a `--ttl` the server rejects, auction-service
  down) it stops handing out requests, lets those in flight finish, and reports why.
  It never replaces an existing manifest with one that lists no auctions.
- **Rerunning adds auctions.** Users are reused; auctions are not. Each run creates
  the full number again, and the new manifest lists only that run's auctions, so
  earlier ones are no longer in any manifest. Use a different `--out` to keep both.
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

| Column                            | Source                                                                           |
| --------------------------------- | -------------------------------------------------------------------------------- |
| `bids_issued`, `bids_per_sec`     | `bids_id_seq`. An upper bound: ids burned by rolled-back transactions count      |
| `orders_issued`, `orders_per_sec` | `orders_id_seq`, same caveat                                                     |
| `outbox_unpublished`              | Rows in `outbox_events` with `published_at IS NULL`                              |
| `outbox_oldest_age_s`             | Age of the oldest unpublished event                                              |
| `outbox_retrying`                 | Unpublished events that already failed at least once, among the oldest 1,000,000 |
| `lock_waiters`                    | Connections waiting on a lock: the signature of a hot auction row                |
| `idle_in_transaction`, `active`   | Connection states, excluding the sampler's own                                   |
| `events_per_sec`                  | Records per second arriving on the topic, all partitions                         |
| `lag_<group>`                     | Records the group has yet to consume, summed over partitions                     |

`partitions.csv`, one row per partition per tick: `start_offset`, `end_offset`,
`produce_per_sec`, and per group `committed_<group>` and `lag_<group>`.

How to read it:

- The sampler never counts the `bids` table, which at hundreds of millions of
  rows would be the load it is watching. Bid and order totals come from sequences,
  which is O(1). The outbox count reads a partial index holding only unpublished
  rows, so its cost would grow with the backlog it reports; it counts at most
  1,000,000 rows, so `outbox_unpublished` equal to 1,000,000 means "at least that
  many". The oldest age comes from the front of the same index and stays exact.
  `outbox_retrying` is counted among those oldest 1,000,000 events, in index
  order, so its scope is the same on every tick; past the cap it can miss retrying
  events further back. Failing events tend to stay near the front, because
  per-auction ordering holds back everything behind them.
- The outbox is read in a statement of its own. If it times out, its three CSV
  cells are empty for that tick and the summary says its peaks may be too low, but
  bids, orders and connections are still recorded.
- A tick waits at most `--read-timeout` (5 s by default) for PostgreSQL and Kafka,
  which is independent of `--interval`; the database's `statement_timeout` is set
  to the same value. A read that times out leaves a gap, and a slow tick delays the
  next one instead of overlapping it.
- A group that has never committed on the topic (or does not exist) has empty
  lag cells, not zero. In a group that has committed somewhere, a partition with
  no commit counts its whole contents as lag, which assumes the group starts at
  the earliest offset. A partition whose offset could not be read is different
  from one with no commit: its cells are empty, and the group's total lag is empty
  for that tick, because a total that skipped a partition would look complete.
- Sampling only reads. It joins no consumer group, so it cannot trigger a
  rebalance.
- Rates divide a counter's change by the time since the last _successful_ reading
  of that source, so a rate after a gap is an average over the gap. A counter that
  went down (a reset database, a recreated topic) gives no rate for that tick.
- The summary's totals (bid and order ids issued, records produced per partition)
  add up the growth between consecutive readings, so a counter that went down
  contributes nothing for that interval, never a negative amount. After a reset
  they can understate what happened, and the summary prints a warning with the
  number of times a counter went backwards.

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
