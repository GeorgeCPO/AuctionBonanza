# Auction Finalization

Status: implemented. Settlement (orders and payments) will consume its events.

Finalization turns an auction whose deadline has passed into a closed one with a
persisted outcome: a winning bid, or no winner when nothing was bid. It is the
first irreversible business fact in the system, so everything that makes it
safe is a database rule, not application discipline.

## What triggers it

The clock, not a message. A bid is a message-shaped fact; a deadline is just a
time passing. So finalization is a worker in `auction-service`, run as its own
process (`finalizer.main.ts`, `start:finalizer`), that polls PostgreSQL. It
cannot be a Kafka consumer: nothing arrives on a topic when `ends_at` passes.
It must also live next to the auction aggregate, because the close and its
outbox event have to commit in one PostgreSQL transaction.

The consumer-side scenarios in `ACCEPTANCE.md` (killing a handler
mid-message, delivering an event five times) belong to the settlement worker
that will consume `AuctionClosed.v1`.

## The close transaction

One transaction per auction (`PostgresFinalizationRepository.finalizeNextDue`):

1. **Claim.** Select the earliest due auction (`cancelled_at IS NULL AND
closed_at IS NULL AND ends_at <= (SELECT clock_timestamp())`, and not waiting
   for a scheduled retry, see [Failed closes](#failed-closes)) with
   `FOR UPDATE SKIP LOCKED`, served by `auctions_pending_finalization_idx`.
   Several finalizers therefore work on different auctions and never queue
   behind each other. An auction locked by an in-flight bid is passed over and
   claimed on the next poll. The clock is written `(SELECT clock_timestamp())`
   on purpose: PostgreSQL then evaluates it once as an InitPlan parameter and
   the `ends_at` bound becomes an index condition. A bare, volatile
   `clock_timestamp()` can only be a filter, which would make every poll walk
   the whole pending index.
2. **Read under the lock.** In a separate statement, read the auction, its
   leading bid and its bidder, and one `clock_timestamp()`. This follows the
   "lock, then evaluate" rule in [`AUCTION_LIFECYCLE.md`](AUCTION_LIFECYCLE.md):
   a bid that committed before the lock is visible, and none can commit after.
3. **Close.** In one `UPDATE`: set `closed_at` and `updated_at` to the observed
   instant, copy `leading_bid_id` into `winning_bid_id`, and advance `version`
   by one.
4. **Emit.** Insert one `AuctionClosed.v1` outbox event carrying the new
   version, in the same transaction. The outbox publisher sends it like any
   other event.

Steps 2 to 4 run inside `SAVEPOINT close_attempt`, followed by
`SET CONSTRAINTS ALL IMMEDIATE`. That makes the deferred "every version has an
outbox event" check fire inside the savepoint, so even that failure is handled
as a failed close instead of failing the commit.

A close is therefore all-or-nothing, and each auction is closed exactly once:
a second finalizer finds `closed_at` set, so the claim query no longer returns
the auction and nothing is written or emitted.

## Failed closes

A close can fail for a reason that retrying will not fix: a constraint or
trigger rejection, an event that breaks its contract, a data problem. The
claim takes the earliest due auction first, so without bookkeeping that one
auction would be claimed again and again and every auction due after it would
wait.

When a close fails, the transaction undoes it with `ROLLBACK TO SAVEPOINT`.
The row lock taken by the claim survives that rollback, so no other finalizer
can touch the auction in between. The finalizer then records the failure on the
auction and commits:

- `finalization_attempts` is incremented,
- `next_finalization_attempt_at` is set to the database clock plus a capped
  exponential backoff with equal jitter, and
- `last_finalization_error` keeps the error text (at most 1000 characters).

The record commits although the close does not. Nothing else changes: not
`version`, `updated_at`, `closed_at` or the outcome. No version trigger fires,
and no outbox event is written or needed. These columns are retry bookkeeping,
like the outbox's publication state, not aggregate state; the API does not
expose them.

The claim and the unlocked pre-check skip an auction until its
`next_finalization_attempt_at`, so it neither blocks the auctions behind it nor
causes empty transactions, and healthy auctions still close in deadline order.
The failed auction is the first claimed again once its retry time passes. The
worker logs each failure at error level (`auction close failed; it will be
retried and does not block other auctions`) with `auctionId`, `attempt` and
`retryDelayMs`, and carries on with the next auction.

Retries are unbounded and the delay is capped (`FINALIZER_AUCTION_RETRY_MAX_DELAY_MS`,
five minutes by default, longer than the loop's own cap because an auction that
keeps failing needs a repair, not a quick retry). There is no dead-letter and no
alert beyond the log line. The auction stays `awaiting_finalization`, which
refuses bids, so it is late but never wrong. A successful close sets
`next_finalization_attempt_at` back to null (a CHECK forbids a scheduled retry on
a closed or cancelled auction) and keeps the attempt count and last error as
history.

Only a failure to record the failure, usually a lost connection, fails the
whole cycle. The loop then backs off as for any database outage.

To find stuck auctions:

```sql
SELECT id, ends_at, finalization_attempts, next_finalization_attempt_at,
       last_finalization_error
FROM auctions
WHERE cancelled_at IS NULL AND closed_at IS NULL AND finalization_attempts > 0
ORDER BY ends_at;
```

To repair one: fix the cause (deploy code or correct the data in a deliberate
script that respects the version and event triggers), then optionally retry
immediately with
`UPDATE auctions SET next_finalization_attempt_at = NULL WHERE id = $1 AND closed_at IS NULL`.
That needs no version step or event. Never close an auction by hand: the
triggers reject a close without its version step and event.

Each claim has to look past the poisoned auctions that are waiting for their
retry, so the cost of a claim grows with their number; the same is true of
the outbox publisher's backed-off heads.

## Unsold is a result

`winning_bid_id IS NULL` on a closed auction means it closed unsold. The event
carries `null` for the winning bid, the winner and the amount.

## What the database enforces

| Rule                                                               | Mechanism                                               |
| ------------------------------------------------------------------ | ------------------------------------------------------- |
| Only a closed auction has a winner                                 | `auctions_winning_bid_closed_check`                     |
| A winner is a bid on the same auction                              | composite foreign key `auctions_winning_bid_id_fkey`    |
| Closing names exactly the leading bid (or none if nothing was bid) | `auction_require_winner_on_close` trigger               |
| A closed auction's outcome never changes                           | the same trigger                                        |
| `closed_at` is not before `ends_at`                                | existing `auctions_closed_at_check`                     |
| A close advances the version by one and carries exactly one event  | existing version triggers ([`REALTIME_GATEWAY.md`][gw]) |
| An auction is never both cancelled and closed                      | existing `auctions_terminal_state_check`                |

The trigger rejects a foreign bid before the foreign key is evaluated; the key
stays as an independent second layer.

[gw]: REALTIME_GATEWAY.md#1-contiguous-auction-versions

## Failure scenarios

| Scenario                                          | What happens                                                                                                                                                                                                                                                                                  |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Finalizer dies before `COMMIT`                    | The transaction rolls back and the lock is released with the session. The auction is still unclosed and is claimed again by any finalizer. No partial close exists.                                                                                                                           |
| Finalizer dies after `COMMIT`                     | The close is durable. The event is in the outbox and is published by the publisher, independently.                                                                                                                                                                                            |
| `COMMIT` succeeds but its acknowledgement is lost | The auction is closed. The next claim does not return it. Nothing is closed twice and nothing is emitted twice.                                                                                                                                                                               |
| Two finalizers race for one auction               | One takes the lock. The other skips it with `SKIP LOCKED` and works on another auction, or finds none due.                                                                                                                                                                                    |
| A bid is in flight at the deadline                | If the bid holds the row lock, the finalizer skips the auction and closes it after the bid commits or rolls back, naming the highest accepted bid. The bid itself is checked against the deadline under the same lock, so a bid arriving after `ends_at` is rejected with `auction_not_open`. |
| A finalizer stalls holding a lock                 | `idle_in_transaction_session_timeout` (30 s) ends its session and releases the row.                                                                                                                                                                                                           |
| The finalizer is down or behind                   | The auction stays in the derived `awaiting_finalization` phase, which already refuses bids. Closing is late, never wrong; the backlog drains in deadline order when a finalizer returns.                                                                                                      |
| One auction's close keeps failing                 | Each failure is recorded on the auction and the auction is skipped until its scheduled retry (see [Failed closes](#failed-closes)). Auctions due after it close normally.                                                                                                                     |
| Database clock differs from the application clock | Only database time is used, both for the claim and for the stamp, so every finalizer agrees.                                                                                                                                                                                                  |

## Downstream

- **Realtime gateway.** The relay forwards `AuctionClosed.v1`. The edge does
  not interpret it: it counts as a version it cannot apply, so the edge
  resyncs from `GET /v1/auctions/live-states` and sends a fresh `snapshot`
  showing the `closed` phase. A dedicated `closed` SSE event is a possible
  follow-up; it is not needed for correctness.
- **Consumers** must follow the rules in
  [`OUTBOX_PUBLISHING.md`](OUTBOX_PUBLISHING.md#consumer-obligations): de-duplicate
  on `eventId` and treat `auctionVersion` as the order.
- **API.** `GET /v1/auctions/:id` and the listing return `winningBidId`. It is
  `null` until the auction closes, and stays `null` if it closed unsold.
  The live-state read does not change.

## Configuration

| Variable                               | Default | Purpose                                                                                                                         |
| -------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`                         | none    | The `auction_db` connection.                                                                                                    |
| `FINALIZER_CONCURRENCY`                | 1       | Parallel loops, each using one connection.                                                                                      |
| `FINALIZER_POLL_INTERVAL_MS`           | 1000    | Pause between polls once nothing is due.                                                                                        |
| `FINALIZER_RETRY_BASE_DELAY_MS`        | 500     | First retry delay after a failed cycle.                                                                                         |
| `FINALIZER_RETRY_MAX_DELAY_MS`         | 30000   | Cap of the exponential backoff.                                                                                                 |
| `FINALIZER_AUCTION_RETRY_MAX_DELAY_MS` | 300000  | Cap of the per-auction backoff after a failed close; at least the cap above. The base delay is `FINALIZER_RETRY_BASE_DELAY_MS`. |
| `FINALIZER_SHUTDOWN_TIMEOUT_MS`        | 15000   | Hard deadline for graceful shutdown; an unfinished close rolls back.                                                            |

The poll interval is also the worst-case lateness of a close after `ends_at`.

## Not covered yet

- Cancellation: there is no endpoint, so no `AuctionCancelled` event. The
  finalizer already skips cancelled auctions.
- Parking an auction after a number of failed attempts, or an alert beyond the
  error log. A failing auction is retried forever with a capped delay.
- Settlement: creating an order and charging the winner.
