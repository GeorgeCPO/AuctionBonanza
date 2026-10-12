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
- Every version change is a single step (`n` to `n + 1`) and commits together
  with exactly one outbox event carrying the new version. The creation version
  has no event. Consumers rely on this to detect a lost event as a version gap,
  and the database enforces it (see
  [`REALTIME_GATEWAY.md`](REALTIME_GATEWAY.md#1-contiguous-auction-versions)).

Time-sensitive transitions must lock the auction row and then evaluate
`clock_timestamp()` in a later statement. PostgreSQL `now()` is fixed at
transaction start, so using it after waiting for a lock could admit a bid whose
real deadline has passed. Putting `clock_timestamp()` in the same statement as
`FOR UPDATE` has the same flaw: the target list is evaluated before the row lock
is taken, so the captured instant predates any wait. Likewise, under READ
COMMITTED a locking statement keeps the snapshot it started with; joined rows
that another transaction committed while it waited are not visible to it, even
though the locked row itself is re-read. Lock in one statement, then read state
and time in the next.

## Bidding contract

- A bid is placed via `POST /v1/auctions/:id/bids`, authenticated; the
  principal is the bidder.
- A bid must meet the current qualifying threshold: the leading bid's amount
  plus `minimum_bid_increment_minor` if the auction already has an accepted
  bid, otherwise `starting_price_minor`. Bids must be strictly positive even
  when `starting_price_minor` is zero, so the first-bid threshold is
  effectively `max(starting_price_minor, 1)`.
- Acceptance locks the auction row with `SELECT ... FOR UPDATE`, then in a
  separate statement reads the auction, its leading bid, and one
  `clock_timestamp()`, and only then validates and inserts, matching the
  "lock, then evaluate" rule above.
  The same captured instant is also written as the accepted bid's
  `created_at` and the auction's resulting `updated_at`, so a bid can never
  be timestamped later than the mutation it caused.
- An accepted bid is immutable: a bid that doesn't qualify is never
  persisted. Acceptance sets `auctions.leading_bid_id` to the new bid and
  increments `version`, then appends one immutable `BidAccepted.v1` payload to
  `outbox_events`, in the same transaction as the bid insert. All three writes
  commit together or all roll back. The event reuses the acceptance timestamp
  and resulting auction version. Rejections and idempotency replays return
  before these writes, so they never append another event. A composite foreign
  key ensures `leading_bid_id` can only reference a bid on that same auction.
- A client-supplied `idempotencyKey`, scoped to (auction, bidder, key), lets a
  retried request return the original outcome instead of being re-evaluated
  against a since-changed price or phase. This closes a real gap: a dropped
  response on a bid that actually qualified, retried after someone else has
  since raised the price, would otherwise look like a "too low" failure even
  though the original bid succeeded. Reusing a key with a different amount is
  a client error, not a replay.
- Automatic/max (proxy) bidding is out of scope; only this simple threshold
  rule is implemented.

The bids model above defines simple bidding. Automatic/max bidding remains
deferred.

## Finalization contract

Finalization is implemented; the details and failure behavior are in
[`FINALIZATION.md`](FINALIZATION.md).

- A worker closes an auction once `ends_at` has passed and it is neither
  cancelled nor closed. It locks the row, then in a later statement reads the
  leading bid and `clock_timestamp()`.
- One transaction sets `closed_at`, copies `leading_bid_id` into
  `winning_bid_id`, advances `version` by one, and appends one
  `AuctionClosed.v1` outbox event with that version.
- `winning_bid_id` is the persisted outcome. It references the same bid that
  led at the close. A closed auction with no `winning_bid_id` closed unsold.
- A closed auction's outcome is immutable, and finalizing it again changes
  nothing and emits nothing.
- A close that fails is recorded on the auction (attempt count, a retry time on
  the database clock, the last error) and retried later with backoff, without
  holding up other auctions. These columns are not aggregate state: writing
  them does not advance `version`, emits no event, and the API does not expose
  them.
