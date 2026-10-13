# Settlement

Status: implemented for orders. Charging the winner is not built yet.

Settlement turns a sold auction into an order for its winner. It is the
consumer of [`AuctionClosed.v1`](FINALIZATION.md), the first business fact the
system produced, and the first consumer that writes to PostgreSQL in response to
a Kafka message. Its job is narrow and its failure rules are strict: one order
per sold auction, for exactly the bidder who won, however the messages arrive.

## What it does

The settlement worker is a Kafka consumer in group `settlement-worker`, in its
own service (`services/settlement-service`). For each record on `auction.events`:

| Record                                                                  | Result                                                                                                                                        |
| ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `AuctionClosed.v1`, sold                                                | One order for the winner, and a record that the event was handled, in one transaction.                                                        |
| `AuctionClosed.v1`, unsold                                              | The record that the event was handled, after checking the auction really is unsold. No order.                                                 |
| `BidAccepted.v1`                                                        | Ignored. Settlement does not own it.                                                                                                          |
| Any other event type                                                    | Ignored, with a warning: a close under a new name (`AuctionClosed.v2`, a mistyped header) would otherwise create no order and leave no trace. |
| A record that claims to be `AuctionClosed.v1` but cannot be read as one | Copied to the dead-letter topic ([below](#the-dead-letter-topic)).                                                                            |
| An `AuctionClosed.v1` the database refuses                              | Copied to the dead-letter topic, with the rule that refused it.                                                                               |

It has no HTTP API. It reads and writes `auction_db`. It **owns** the
`orders` and `settlement_processed_events` tables, and reads `auctions` and
`bids` only through a foreign key and a validating trigger (see
[the database](#what-the-database-enforces)). That is a deliberate step toward
the stronger ownership boundaries `ARCHITECTURE.md` calls for, not a database of
its own.

## Why a separate service

Finalization had to live inside `auction-service` because the close and its
outbox event must commit in one PostgreSQL transaction with the auction
aggregate. Settlement has no such constraint. It reacts to a committed fact, so
it can run, scale and fail independently: a slow or broken settlement worker
cannot slow a bid or a close, and a bug in it cannot corrupt the auction
aggregate. The price is the message delivery problems the rest of this
document is about.

## The message handler

```text
record from auction.events
   |
   v
decideSettlement(key, value, headers)            pure, no I/O
   |-- ignore ------------------------------------> acknowledge
   |-- reject (cannot be read) --+
   |-- settle ------------------> repository.settle(event)     one transaction
   |                                 |-- committed ------------> acknowledge
   |                                 |-- PermanentSettlementError --+
   |                                 `-- any other error ------> retry (partition paused)
   |
   `-- dead-letter: publish the original record, wait for the broker's ack
          |-- acknowledged ----------------------------------> acknowledge
          `-- failed ----------------------------------------> retry (partition paused)
```

**Acknowledging** means returning from the handler. The Kafka client stores a
message's offset only after its handler resolves, and auto-commit commits only
stored offsets, so an offset is never committed before the effect it stands for.

### The transaction

`PostgresSettlementRepository.settle` runs one transaction per event:

1. `INSERT INTO settlement_processed_events ... ON CONFLICT DO NOTHING`, with no
   conflict target on purpose: the event is unique under two keys, and a target
   would let a race hit the other key and raise instead of being skipped. No row
   back means an earlier delivery handled this event: commit, report
   `duplicate`. This is the consumer rule from
   [`OUTBOX_PUBLISHING.md`](OUTBOX_PUBLISHING.md#consumer-obligations): de-duplicate
   on `eventId`.
2. For a sold auction, `INSERT INTO orders`. For an unsold one there is no order
   for a trigger to check, so the auction's recorded outcome is read in the same
   transaction: an event that says "unsold" about an auction with a winner (or one
   that is not closed, or does not exist) is refused, as a permanent failure, and
   not recorded as handled.
3. `COMMIT`.

The record and the order commit together or not at all, so there is never an
event that counts as handled with no order, or an order whose event is not
recorded. Two deliveries of one event racing each other serialize on the
primary key of the first insert: the second waits for the first to commit, then
sees its row.

### Two layers of idempotency

| Layer                                       | Guards against                                            | Failure mode                                                                             |
| ------------------------------------------- | --------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `settlement_processed_events.event_id` (PK) | The same event delivered again (the normal case).         | Recognised, nothing written, acknowledged.                                               |
| `orders.auction_id` (unique)                | A second, _different_ event for one auction (an anomaly). | Fails with `23505`, rolls back, and is dead-lettered, because someone should look at it. |

The first is the documented rule and makes redelivery silent. The second is the
business invariant, "one order per auction", enforced where no bug in the
consumer can bypass it. `auctionVersion` is stored for audit only: an auction's
close is its last event, so settlement needs no version ordering to decide
anything.

## What the database enforces

The consumer is a convenience; these rules hold for any writer, including a
person with `psql`.

| Rule                                                                                                      | Mechanism                                                     |
| --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| One order per auction                                                                                     | `orders_auction_id_key`                                       |
| An order exists only for a closed auction                                                                 | `order_require_winning_bid` trigger (`orders_auction_closed`) |
| The order names the auction's recorded winning bid                                                        | the same trigger (`orders_winning_bid_is_auction_winner`)     |
| The buyer, price and currency are exactly the winning bid's bidder and amount, and the auction's currency | the same trigger (`orders_terms_match_winning_bid`)           |
| The winning bid belongs to the same auction                                                               | composite foreign key `orders_winning_bid_id_fkey`            |
| An order exists only because an event was processed, and that event was for the order's own auction       | composite foreign key `orders_source_event_id_fkey`           |
| An order's facts never change                                                                             | `order_reject_fact_change` trigger (`orders_facts_immutable`) |

This is "no incorrect winner" as a database rule. The trigger compares the order
with the auction's recorded outcome, which `auctions_closed_outcome_immutable`
makes immutable once the auction closes, so the check needs no lock. It runs
before the foreign keys are evaluated, which is why a foreign key is also
tested on its own, with the trigger switched off inside a rolled-back
transaction.

## Failures

### What is retried and what is dead-lettered

A failure is **permanent** if retrying cannot change the answer, and
**transient** otherwise. The two get opposite treatment, and the default errs
toward retrying:

| Failure                                                                                                                                         | Kind      | Treatment                                |
| ----------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---------------------------------------- |
| The record cannot be read as an `AuctionClosed.v1` (bad JSON, contract violation, key or `event-id` mismatch, no `event-type` header, no value) | permanent | Dead-letter, then acknowledge.           |
| The database refuses the data: SQLSTATE class `22` (data exception) or `23` (integrity)                                                         | permanent | Dead-letter, then acknowledge.           |
| An "unsold" event about an auction the database records as sold, open, or missing                                                               | permanent | Dead-letter, then acknowledge.           |
| The database is down, a timeout, a deadlock, a serialization failure, a lost connection                                                         | transient | Pause the partition, retry with backoff. |
| The dead-letter topic is unavailable                                                                                                            | transient | Pause the partition, retry with backoff. |
| Any error settlement does not recognise                                                                                                         | transient | Pause the partition, retry with backoff. |

Retrying an error that is really permanent stalls one partition, loudly, in the
logs. Dead-lettering an error that is really transient would leave a winner
without an order until a person noticed. The first is the safer mistake.

A **transient** failure pauses the partition, throws, and the client seeks back
to the failed record, which is delivered again after a capped exponential
backoff with jitter. Pausing, rather than waiting inside the handler, keeps the
consumer polling, so a long outage does not exceed `max.poll.interval.ms` and
evict the worker from the group. Records behind the failed one on that partition
wait; other partitions keep flowing. Nothing is skipped and nothing is
committed past the failure.

### The dead-letter topic

`auction.events.dlq`: one partition, 30 days of retention (longer than the main
topic's 7, because it exists for repair). It is provisioned by `kafka-init`; a
worker never creates it.

A dead-lettered record is the original record, unchanged, with headers added:

| Header                 | Value                                                                                                                                                                     |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dlq-reason`           | `missing-event-type`, `missing-value`, `invalid-json`, `contract-violation`, `key-mismatch`, `event-id-mismatch`, or `database-rejected`                                  |
| `dlq-detail`           | What was wrong (the first rule of the `AuctionClosed.v1` contract the record breaks, or the database's message with its SQLSTATE and constraint), at most 1000 characters |
| `dlq-source-topic`     | The topic it was read from                                                                                                                                                |
| `dlq-source-partition` | Its partition                                                                                                                                                             |
| `dlq-source-offset`    | Its offset, as a decimal string                                                                                                                                           |
| `dlq-consumer-group`   | The group that rejected it                                                                                                                                                |
| `dlq-failed-at`        | When, as an ISO 8601 UTC timestamp                                                                                                                                        |

The key, value and the record's own headers are copied byte for byte, so a
person repairing it sees what the producer actually sent. A dead-letter send
resolves only after an `acks=all` acknowledgement, and only then is the source
offset stored.

**The dead-letter topic is at-least-once.** A crash after the broker
acknowledged the copy and before the source offset was committed redelivers the
source record and dead-letters it a second time. The source coordinates in the
headers make that duplicate recognisable.

To read it:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic auction.events.dlq --from-beginning \
  --property print.key=true --property print.headers=true
```

Nothing consumes the dead-letter topic yet, and nothing alerts on it. There is no
replay tool: after the cause is fixed, a person re-produces the record to
`auction.events` or repairs the data and re-runs settlement for the auction.

### Crash and loss windows

| What happens                                                    | Result                                                                                                                                | Proved by                                                  |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| The worker dies before `COMMIT`                                 | The transaction rolls back. The offset was not stored, so the record is delivered again and settled once.                             | `settlement-repository` (session killed before `COMMIT`)   |
| The worker dies after `COMMIT`, before the offset is committed  | The record is delivered again. The event is recorded, so it is a `duplicate`. No second order.                                        | `settlement-process` (SIGKILL with a 60 s commit interval) |
| `COMMIT` succeeds but its acknowledgement is lost               | The handler throws and the record is retried. The retry finds the event recorded: `duplicate`.                                        | `settlement-repository` (`COMMIT` applied, reply lost)     |
| The same event is delivered five times                          | One order. Four `duplicate` outcomes.                                                                                                 | `settlement-repository`, `settlement-consumer`             |
| The same event is handled by several workers at the same moment | One order. They serialize on the event's primary key.                                                                                 | `settlement-repository` (ten at once)                      |
| PostgreSQL is unavailable                                       | The partition pauses at the failing record, nothing later is settled or committed, and it resumes in order when the database returns. | `settlement-consumer` (database unavailable)               |
| The dead-letter topic is unavailable                            | Same: the partition pauses on the bad record. Nothing is lost and nothing behind it is settled early.                                 | `settlement-consumer` (dead-letter topic unavailable)      |
| A new consumer group is started                                 | It reads the retained history from the beginning. Every event is a `duplicate`. No second order.                                      | `settlement-consumer` (new group)                          |
| A worker is restarted cleanly                                   | It resumes from the committed offsets and is given only new records.                                                                  | `settlement-consumer` (restart)                            |

All of this is in `services/settlement-service/test/integration/`; the
end-to-end path from a bid to an order is `tests/e2e/test/settlement.test.ts`.

## Ordering and scaling

All events of an auction share a key and so a partition. A partition is handled
by one worker at a time, one record at a time, in offset order, and a failed
record is retried before any later one. A worker handles
`SETTLEMENT_PARTITION_CONCURRENCY` partitions at once, each holding at most one
database connection, so that setting is also the pool size. Workers scale by
joining the group, up to the topic's partition count (12).

Ordering across auctions is irrelevant to settlement.

## Known limits

- **A closed auction can go unsettled if its event is lost.** If the group is
  more than the topic's 7-day retention behind, or its offsets expire after it
  has been empty for longer than the broker's `offsets.retention.minutes`, an
  `AuctionClosed.v1` can be deleted before it is read, and its auction never
  gets an order. The database is the source of truth, so the check is a query:

  ```sql
  SELECT a.id, a.closed_at, a.winning_bid_id
  FROM auctions a
  LEFT JOIN orders o ON o.auction_id = a.id
  WHERE a.winning_bid_id IS NOT NULL AND o.id IS NULL
  ORDER BY a.closed_at;
  ```

  An auction that closed seconds ago also appears while its event is in flight,
  so only a row that stays is a problem. Nothing runs this query, and nothing
  settles the auctions it finds; a reconciler is the next step if this matters.

- **Settlement reads all bid traffic.** `auction.events` carries every event
  type on one topic, so the worker reads and discards each `BidAccepted.v1`, and
  lag on bids delays settlement. Hot-auction behaviour is measured elsewhere
  ([`ACCEPTANCE.md`](../ACCEPTANCE.md)); a filtered topic or stream would be the
  fix if this becomes the bottleneck.
- **A permanently bad record stays on the dead-letter topic until a person acts.**
  Settlement keeps going, but that auction has no order. There is no alert.
- **The order carries no seller.** `AuctionClosed.v1` has none, and the order
  stores `auction_id`, from which the seller (`auctions.owner_id`) is derivable.
  A payout would need the contract to carry it, under the versioning rules for
  events already in Kafka.
- **A duplicate dead letter is possible** (see above).

## Not covered yet

- Charging the winner. An order is created in its only state; a payment status
  is added with the payment milestone, and `order_reject_fact_change` already
  allows columns beyond the facts to change.
- An API to read orders.
- Cancellation (there is no `AuctionCancelled` event yet).
- A dead-letter replay tool, a consumer of the dead-letter topic, and alerting.
- The reconciler described above.

## Configuration

Copy `services/settlement-service/.env.settlement.example` to `.env.settlement`.
Every variable has a default except `DATABASE_URL`; an invalid value stops the
process at startup, naming it.

| Variable                            | Default              | Purpose                                                                                                                                                                                                         |
| ----------------------------------- | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`                      | none                 | The `auction_db` connection.                                                                                                                                                                                    |
| `KAFKA_BROKERS`                     | `localhost:9092`     | Comma-separated `host:port` list.                                                                                                                                                                               |
| `KAFKA_CLIENT_ID`, `KAFKA_GROUP_ID` | `settlement-worker`  | Client id and consumer group. Workers sharing a group share the partitions.                                                                                                                                     |
| `AUCTION_EVENTS_TOPIC`              | `auction.events`     | The topic consumed.                                                                                                                                                                                             |
| `SETTLEMENT_DEAD_LETTER_TOPIC`      | `auction.events.dlq` | Where unprocessable records go. Must differ from the topic consumed, or a rejected record would feed back to the worker that rejected it. Must already exist: the worker refuses to start otherwise, naming it. |
| `SETTLEMENT_PARTITION_CONCURRENCY`  | 4                    | Partitions handled at once; also the database pool size.                                                                                                                                                        |
| `SETTLEMENT_DEAD_LETTER_TIMEOUT_MS` | 5000                 | Bounds one dead-letter send, so a failed send is retried instead of hanging a partition. At least 1000: librdkafka rejects the connection timeout derived from it below that.                                   |
| `SETTLEMENT_SESSION_TIMEOUT_MS`     | 10000                | How long a crashed worker keeps its partitions before the group reassigns them. The broker's floor is 6000.                                                                                                     |
| `SETTLEMENT_COMMIT_INTERVAL_MS`     | 1000                 | How often stored offsets are committed. Longer widens the window in which a crash redelivers handled records, which is harmless.                                                                                |
| `SETTLEMENT_RETRY_BASE_DELAY_MS`    | 200                  | First retry delay after a transient failure.                                                                                                                                                                    |
| `SETTLEMENT_RETRY_MAX_DELAY_MS`     | 10000                | Cap of the exponential backoff.                                                                                                                                                                                 |
| `SETTLEMENT_SHUTDOWN_TIMEOUT_MS`    | 15000                | Hard deadline for graceful shutdown; at least the dead-letter timeout plus one second. Unfinished work rolls back and is redelivered.                                                                           |

Database sessions are limited to a 10 s statement timeout, a 5 s lock timeout
and a 30 s idle-in-transaction timeout, so a stalled worker releases its locks.
