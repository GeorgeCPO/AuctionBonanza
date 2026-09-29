# Outbox Publishing

Status: accepted for the accepted-bid event path. Gateway consumption and SSE
delivery are out of scope here.

Bid acceptance appends a `BidAccepted.v1` row to `outbox_events` in the same
transaction that accepts the bid (see
[`AUCTION_LIFECYCLE.md`](AUCTION_LIFECYCLE.md)). The outbox publisher is a
separate worker process that moves those committed rows to Kafka.

## Guarantees in one paragraph

Every committed outbox event is published to Kafka **at least once**. It is
never marked published unless Kafka acknowledged it with `acks=all`, and it is
never dropped because publication failed. Messages for one auction share a key
(`auctionId`), so they share a partition. The publisher does not send an event
until every earlier event of the same auction has been acknowledged and marked
published. Duplicates are possible, and a duplicate can arrive **after** later
events of the same auction. This design does **not** provide exactly-once
delivery, and the raw `auctionVersion` sequence on a partition is not guaranteed
to strictly increase. Consumers must de-duplicate by `eventId` and apply an
event only if its `auctionVersion` is newer than the last one they applied.

## Topic and message

| Item            | Value                                                                                    |
| --------------- | ---------------------------------------------------------------------------------------- |
| Topic           | `auction.events` (`AUCTION_EVENTS_TOPIC`), 12 partitions locally, provisioned in Compose |
| Key             | `auctionId` as a decimal string                                                          |
| Value           | The stored `payload` exactly as PostgreSQL renders the `jsonb` value                     |
| Headers         | `event-id`, `event-type`, `content-type: application/json`                               |
| Partitioner     | `murmur2_random`, which matches the Java client's keyed hashing                          |
| Producer safety | `acks=all`, `enable.idempotence=true`                                                    |

One topic carries all auction events, not one topic per event type. That way,
future events such as cancellation or closure share a partition with the bids
of the same auction and keep a single per-auction order. Consumers dispatch on
`event-type`.

The value is taken from the database row. It is not re-serialized from a
domain object, so every attempt sends the same `eventId` and a byte-identical
payload. PostgreSQL canonicalizes `jsonb`, so key order and whitespace can
differ from the JSON text written at acceptance time. The content is the same.
The immutability trigger on `outbox_events` guarantees that the event data
cannot change between attempts.

## Claiming: concurrency and per-auction order

Each publication cycle claims a batch with one statement
(`services/auction-service/src/outbox/outbox.store.ts`):

```sql
SELECT ... FROM outbox_events AS candidate
WHERE candidate.published_at IS NULL
  AND (candidate.next_publish_attempt_at IS NULL
       OR candidate.next_publish_attempt_at <= clock_timestamp())
  AND NOT EXISTS (
    SELECT 1 FROM outbox_events AS predecessor
    WHERE predecessor.auction_id = candidate.auction_id
      AND predecessor.published_at IS NULL
      AND predecessor.auction_version < candidate.auction_version)
ORDER BY candidate.occurred_at, candidate.event_id
LIMIT $1
FOR UPDATE OF candidate SKIP LOCKED
```

- **Only heads are eligible.** An event qualifies only if no earlier event of
  its auction is still unpublished. The predecessor check deliberately ignores
  both retry scheduling and row locks. A predecessor that is waiting to retry,
  or is locked by another publisher that is sending it right now, still blocks
  its successors.
- **Other auctions keep moving.** `SKIP LOCKED` skips heads that another
  publisher holds, and skipped rows do not count toward `LIMIT`. One stuck
  auction therefore does not stall the batch. A batch contains at most one
  event per auction.
- **No race between send and commit.** While publisher A holds head `E1`, the
  row is unpublished in every snapshot, so publisher B cannot treat `E2` as a
  head. Once A commits, `E1` is published in every later snapshot. If B's
  snapshot predates that commit, B finds `E1` unlocked but re-checks
  `published_at IS NULL` against the committed row version and excludes it.
- **Version order equals visibility order, enforced by the database.**
  `outbox_events_auction_version_key` allows one event per auction version,
  so versions totally order an auction's events with no ties. The
  `outbox_events_current_auction_version` trigger locks the auction row
  (`FOR NO KEY UPDATE`) on every outbox insert and rejects any version other
  than the auction's current one. A writer must therefore bump
  `auctions.version` in the same transaction, and it holds the auction lock
  until commit. The writer of version `n + 1` cannot read the auction until
  the writer of version `n` has committed, so no publisher can see a later
  version without its predecessor. This applies to every future outbox
  writer, not just bid acceptance.

The partial index `outbox_events_unpublished_idx (occurred_at, event_id)`
drives the ordered scan. The partial index
`outbox_events_unpublished_auction_idx (auction_id, auction_version)`
answers the predecessor check. Both indexes contain only unpublished rows.

The ordered scan still visits rows it cannot claim, such as rows locked by
other publishers, successors, or rows not yet due. A large backlog that is
mostly blocked makes each claim more expensive. This is acceptable at the
current scale, and it is worth measuring when load-generation work begins.

## Transaction and acknowledgement boundary

One publication cycle is one PostgreSQL transaction:

1. `BEGIN`, then claim and lock a batch.
2. Send every claimed event to Kafka concurrently and wait for each
   acknowledgement or failure.
3. Set `published_at` for acknowledged events. For failed events, increment
   `publish_attempts` and set `next_publish_attempt_at` and
   `last_publish_error`.
4. `COMMIT`.

`published_at` is written only after the acknowledgement. The row lock is held
from the claim through the commit, so no other publisher can send the same
event, or its successor, during that time.

### Database connection tradeoff

Holding the lock through publication means each concurrent publication loop
keeps **one pooled connection inside an open transaction for a whole Kafka
round trip**. The costs:

- Connections: the pool is sized to exactly `OUTBOX_PUBLISHER_CONCURRENCY`.
  Throughput scales with loops and batch size, not with extra connections.
- MVCC horizon: an open transaction holds back vacuum for as long as it lasts.
  The publish timeout bounds that duration. During a Kafka outage, every loop
  holds a transaction for up to one timeout per cycle.
- Session loss: if the database terminates the session while it waits,
  `node-postgres` would normally emit an unhandled `'error'` event on the
  checked-out client. The shared pool wrapper
  (`services/auction-service/src/database.ts`) catches that
  error and raises it from the next query instead, so the process does not
  crash.

The alternative is a lease: mark rows `claimed_until = now() + lease`, commit,
publish without a transaction, then mark published. This frees connections, but
publication can outlive the lease, so a second publisher can send the same
event, or its successor, while the first send is still in flight. That creates
more duplicates and reorders messages in the normal case, not just after
failures. Row locks make the normal case clean. Duplicates and reordering then
come only from the failure cases below.

### Timeouts

| Setting                                    | Default    | Purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ------------------------------------------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `OUTBOX_PUBLISH_TIMEOUT_MS`                | 10000      | librdkafka `message.timeout.ms`: the delivery deadline, including internal retries. The publisher also races each send against its own timer set to this value plus 2 s, because librdkafka detects expired messages only periodically. During testing, a 3 s timeout was reported after about 3.3 s; with default reconnect backoff it took more than 9 s.                                                                                             |
| `OUTBOX_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS` | 30000      | PostgreSQL releases the locks of a publisher that crashed or stalled. It must be at least the publish timeout plus 5 s, and startup rejects smaller values. If PostgreSQL could end the transaction first, a healthy publisher would lose its claim while Kafka might still acknowledge.                                                                                                                                                                |
| `OUTBOX_SHUTDOWN_TIMEOUT_MS`               | 30000      | Hard deadline for graceful shutdown, starting at the signal. It must be at least the publish timeout plus 7 s: the publisher's 2 s timer grace plus 5 s for the in-flight cycle's bookkeeping and `COMMIT`. A shorter deadline would predictably force an exit mid-cycle during a slow send, rolling back events Kafka already acknowledged. The producer flush afterwards only gets the time that remains, because every outcome is committed by then. |
| `statement_timeout` / `lock_timeout`       | 10 s / 5 s | Fixed values for the publisher's short claim and update statements. `SKIP LOCKED` never waits for row locks.                                                                                                                                                                                                                                                                                                                                            |

## Retries

A failed send does not fail the cycle. The event keeps its row and its lock
until commit. It is recorded with `publish_attempts + 1`, a
`last_publish_error` of at most 1000 characters, and a `next_publish_attempt_at`
computed with **capped exponential backoff and equal jitter**. For attempt `n`,
the delay is drawn uniformly from `[c/2, c]`, where
`c = min(OUTBOX_RETRY_MAX_DELAY_MS, OUTBOX_RETRY_BASE_DELAY_MS * 2^(n-1))`.
The jitter spreads out retries of events that failed together during one
outage. The fixed half guarantees a real gap between attempts. The database
clock schedules retries, so every publisher agrees on when an event is due.

Retries are **unbounded in count**. An event is never discarded, parked, or
marked published after a failure, and it keeps blocking later events of its
auction until it succeeds. Every failure logs, at error level, the `eventId`,
`eventType`, `auctionId`, `auctionVersion`, attempt number, `retryDelayMs`, and
the underlying error.

If a whole cycle fails, for example because PostgreSQL is unavailable, the loop
backs off with the same policy and claims again. Any claims from the failed
cycle roll back.

Some events can never succeed: an unknown `event_type`, or a payload that no
longer satisfies its contract. These are retried the same way and logged as
`outbox event cannot be published; it blocks later events for its auction until
repaired`. Repairing one is a deliberate operator decision, for example
deploying code that understands the event. This design has no automatic
dead-letter path.

## Failure scenarios

| Scenario                                                                                                | What happens                                                                                                                                                                                                                                                                    | Consumer sees                                                     |
| ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| Kafka unreachable                                                                                       | The send times out within the publish timeout (plus grace). The failure is recorded and retried with backoff. The auction's later events wait, and other auctions keep trying. The same producer reconnects when Kafka returns (reconnect backoff is at most 1 s).              | Nothing until recovery, then the events in order.                 |
| Kafka acknowledged but the process dies, or loses PostgreSQL, before `COMMIT`                           | The transaction rolls back and the lock is released with the session. The event is unpublished and is republished with the same ID and payload. The successor waits for the republished copy.                                                                                   | `v, v, v+1`: an **adjacent duplicate**.                           |
| Publisher timer or librdkafka gives up, but Kafka still persists the message later                      | The event is recorded as failed and republished after backoff. The successor can be sent after the republished copy, before the original arrives. A late acknowledgement is logged as a warning.                                                                                | Possibly `v, v+1, v`: a **late duplicate** after a newer version. |
| A publisher stalls, for example with a long pause or network partition, and PostgreSQL ends its session | `idle_in_transaction_session_timeout` releases the lock. Another publisher can publish `v` and `v+1` while the stalled send is still pending in the first producer.                                                                                                             | Possibly `v, v+1, v`: a late duplicate.                           |
| Commit succeeds but the commit acknowledgement is lost                                                  | The rows are committed as published. The cycle reports an error, logs that the commit outcome is unknown and that the acknowledged events _may_ be published again, and the loop backs off. Nothing is republished here; if the commit had failed instead, the events would be. | Exactly one copy.                                                 |
| Graceful shutdown (`SIGTERM`/`SIGINT`)                                                                  | No new cycle starts. In-flight cycles finish and commit within the shutdown deadline. The producer then flushes for whatever time remains, and the pool closes.                                                                                                                 | Exactly one copy.                                                 |
| Shutdown deadline exceeded, or a second signal                                                          | The process exits. Uncommitted claims roll back and are republished later.                                                                                                                                                                                                      | Possibly an adjacent duplicate.                                   |
| Kafka unreachable at startup                                                                            | Producer `connect()` fails after librdkafka's metadata wait (about 30 s). The process exits with code 1 and the supervisor restarts it.                                                                                                                                         | Nothing yet.                                                      |

Idempotent production helps only within one producer session: it removes the
producer's own internal retries and keeps per-partition order among messages in
flight. It cannot recognize a republication after a lost outbox transaction,
because that is a new send. Kafka transactions cannot make the PostgreSQL
update and the Kafka write atomic either, because they are two separate systems.

## Consumer obligations

- De-duplicate on `eventId` (header `event-id` or payload `eventId`).
- Treat `auctionVersion` as the order. Ignore an event whose version is less
  than or equal to the last version applied for that auction. A late duplicate
  is then harmless.
- After at-least-once processing with a crash, expect to re-read messages that
  were already processed. Consumer idempotency covers that too.

## Tests

The publisher's integration tests run against real PostgreSQL and Kafka, in
`services/auction-service/test/`: `outbox-publisher-ordering.integration.test.ts`
covers claiming and per-auction order, and
`outbox-publisher-failures.integration.test.ts` covers failures, crashes, and
shutdown. Both share `test/support/outbox-publisher-harness.ts`. Each test
file uses a throwaway database
created from `db/schema.sql`, because a publisher claims every event in its
database and other test suites share `auction_db`. Each test uses its own
topic. Coordination is deterministic: a gate holds a publisher after its claim,
explicit row locks stand in for other publishers, and `ScriptedTransport` wraps
the real Kafka transport to inject failures or to kill the publisher's session
right after Kafka acknowledges. A send held at a gate proves the publisher's
own timeout bound from both sides, and releasing it afterwards reproduces the
late acknowledgement and the resulting duplicate. Retry timing is controlled by
making retries due, not by sleeping.

The broker outage test uses the Compose broker's test-only `FAULT_INJECTION`
listener, which advertises `localhost:19092`. The test runs a TCP proxy on that
address to `localhost:19093`, then cuts and restores every connection of an
already connected producer. A proxy in front of the normal listener would not
work: after the first metadata response, the client would connect directly to
the address the broker advertises.

`outbox-publisher-process.integration.test.ts` builds and runs the worker as a
separate OS process, checks that it rejects invalid configuration, and checks
that it drains cleanly on `SIGTERM`.
