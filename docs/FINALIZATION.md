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
closed_at IS NULL AND ends_at <= clock_timestamp()`) with
   `FOR UPDATE SKIP LOCKED`, served by `auctions_pending_finalization_idx`.
   Several finalizers therefore work on different auctions and never queue
   behind each other. An auction locked by an in-flight bid is passed over and
   claimed on the next poll.
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

A close is therefore all-or-nothing, and each auction is closed exactly once:
a second finalizer finds `closed_at` set, so the claim query no longer returns
the auction and nothing is written or emitted.

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
| PostgreSQL unavailable                            | The cycle fails and the loop retries with capped exponential backoff with jitter (`FINALIZER_RETRY_*`).                                                                                                                                                                                       |
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

| Variable                        | Default | Purpose                                                              |
| ------------------------------- | ------- | -------------------------------------------------------------------- |
| `DATABASE_URL`                  | none    | The `auction_db` connection.                                         |
| `FINALIZER_CONCURRENCY`         | 1       | Parallel loops, each using one connection.                           |
| `FINALIZER_POLL_INTERVAL_MS`    | 1000    | Pause between polls once nothing is due.                             |
| `FINALIZER_RETRY_BASE_DELAY_MS` | 500     | First retry delay after a failed cycle.                              |
| `FINALIZER_RETRY_MAX_DELAY_MS`  | 30000   | Cap of the exponential backoff.                                      |
| `FINALIZER_SHUTDOWN_TIMEOUT_MS` | 15000   | Hard deadline for graceful shutdown; an unfinished close rolls back. |

The poll interval is also the worst-case lateness of a close after `ends_at`.

## Not covered yet

- Cancellation: there is no endpoint, so no `AuctionCancelled` event. The
  finalizer already skips cancelled auctions.
- A dead-letter or alert for a close that fails repeatedly. Failures are
  logged and retried indefinitely, and because the claim always takes the
  earliest due auction first, one auction whose close keeps failing also
  stalls every auction due after it. This differs from the outbox publisher,
  where a stuck event blocks only its own auction. Fixing it needs a failure
  marker with a retry time on the auction, and so a migration.
- Settlement: creating an order and charging the winner.
