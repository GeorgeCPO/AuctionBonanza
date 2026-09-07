# Auction Lifecycle

Status: accepted initial model; automatic/max bidding and settlement will extend it.

## Persist facts, derive time phases

The database does not store `scheduled`, `open`, or `awaiting_finalization` as
mutable status values. Those phases are derived from the schedule using the
database clock. Cancellation and closure are persisted because they are
irreversible business facts.

Phase precedence is:

1. `cancelled_at IS NOT NULL`: `cancelled`
2. `closed_at IS NOT NULL`: `closed`
3. database time is before `starts_at`: `scheduled`
4. database time is before `ends_at`: `open`
5. otherwise: `awaiting_finalization`

`awaiting_finalization` deliberately exposes the interval between the bidding
deadline and a successful finalization transaction. It must not accept bids,
even though `closed_at` has not been written yet.

## Creation contract

- The authenticated principal supplies `owner_id`; request bodies cannot choose
  an owner.
- Titles and descriptions are trimmed, non-empty, and limited to 160 and 10,000
  characters respectively.
- `currency_code` is an uppercase three-letter code. The service owns the list
  of supported ISO 4217 currencies; the database constraint only protects the
  stable representation.
- Monetary values use minor units stored as PostgreSQL `bigint` and cross the
  JSON boundary as decimal strings. Starting price may be zero; minimum bid
  increment must be positive.
- Auction duration is inclusive at the one-minute and thirty-day limits.
- A missing `starts_at` uses database time. For this phase, supplied start times
  in the past are accepted, including schedules that have already ended. The
  database still enforces schedule ordering and duration.
- Creation idempotency is deferred. No request key is required or stored, and
  repeated submissions can create separate auctions. Clients should avoid
  automatically retrying creation when the outcome is unknown.
- Creation uses one atomic `INSERT ... RETURNING` statement to save and return
  the auction. Database constraints enforce schedule ordering and duration.
  Reads and creation return a database timestamp used to derive the phase.

The reshape migration backfills pre-contract rows with a title derived from the
description, `USD` as a migration-only currency decision, a one-minor-unit bid
increment, version 1, and `updated_at = created_at`. New writes receive no
currency or bid-increment default and must state both values.

## Transitions and concurrency

- A scheduled auction may be cancelled by its owner.
- An open auction may be cancelled only before its first accepted bid.
- Once a bid has been accepted, owner cancellation is forbidden.
- Bids are valid only in the half-open interval `starts_at <= time < ends_at`.
- The owner cannot bid on their own auction.
- Finalization is allowed only at or after `ends_at` and is idempotent.
- Every externally visible aggregate mutation increments `version`.

Time-sensitive transitions must lock the auction row and then evaluate
`clock_timestamp()`. PostgreSQL `now()` is fixed at transaction start, so using
it after waiting for a lock could admit a bid whose real deadline has passed.

Winner and displayed-price fields are intentionally deferred until the bids
model defines simple and automatic/max bidding semantics. Finalization will
eventually persist either an unsold result or a winning-bid reference in the
same transaction as its outbox event.
