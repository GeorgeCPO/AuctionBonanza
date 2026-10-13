# Database Migrations

Raw SQL migrations are the executable schema source of truth. `../schema.sql`
is a generated snapshot of the database after all migrations have been applied.
Do not edit the snapshot by hand.

Dbmate runs migrations centrally for the shared `auction_db` database. Run its
commands from the repository root so it can read `DATABASE_URL` from the root
`.env` file:

```sh
cp .env.example .env
pnpm db:status
pnpm db:migrate
```

`db:migrate` and `db:rollback` explicitly regenerate `db/schema.sql` after the
database change. Use `pnpm db:dump` to regenerate it without changing the
database. Schema generation requires a PostgreSQL `pg_dump` executable on
`PATH`; its major version should be at least as new as the database server's.
The explicit dump makes the command fail visibly instead of silently leaving a
stale snapshot when the client tool is missing. To use the Compose postgres
container's `pg_dump` instead, as CI does, run
`PATH="$PWD/scripts/compose-bin:$PATH" pnpm db:migrate`.

Use `pnpm db:new <owner>__<change>` to create a migration. Dbmate requires a
leading numeric version, so committed files follow
`<timestamp>_<owner>__<change>.sql`; for example,
`20260902193000_identity__create_users.sql`. The owner prefix in the descriptive
part makes table ownership visible while the timestamp gives every migration a
globally ordered version in the shared database.

The initial `users` migration makes these schema decisions explicit:

- `id` is `bigint GENERATED ALWAYS AS IDENTITY`. PostgreSQL owns ID generation;
  exceptional imports can still opt in with `OVERRIDING SYSTEM VALUE`.
- `created_at` is generated in the database with `DEFAULT now()` and is not
  nullable, so every writer gets the same behavior.
- Username and email uniqueness is enforced by unique indexes on `lower(...)`.
  This preserves the submitted spelling without requiring the `citext`
  extension while rejecting values that differ only by case. Application input
  validation and normalization remain separate concerns.

The initial `auctions` migration follows the same identity and `created_at`
conventions. It also:

- references `users(id)` from `owner_id` without cascading deletes, so a user
  cannot be deleted while owned auctions still exist;
- checks that `end_date` is later than `start_date`;
- indexes `owner_id` for owner lookups and `end_date` for closing-auction scans;
- stores `starting_price` as a `bigint` minor-unit amount. Its allowed range is
  intentionally left to a later business rule because the source model only
  requires a value.

The auction reshape migration turns that seed table into the initial lifecycle
aggregate. It adds bounded, trimmed presentation fields; an explicit currency
representation; non-negative starting-price and positive-increment checks; a
one-minute to thirty-day duration; aggregate versioning; persisted cancellation
and closure facts; and timestamp-order constraints. Scheduled, open, and
awaiting-finalization remain derived phases, as documented in
[`../../docs/AUCTION_LIFECYCLE.md`](../../docs/AUCTION_LIFECYCLE.md).

The auction listing index orders `created_at` and the `id` tie-breaker together,
matching the public auction keyset query. Title substring search deliberately
uses no additional index yet: a normal B-tree cannot support an unanchored,
case-insensitive substring efficiently, and introducing trigram or full-text
search infrastructure is deferred until search workload justifies it.

The bids migration adds the `bids` table and the `auctions` ↔ `bids` link.
`bids` rows are immutable — only a bid that qualifies under the current price
is ever inserted, so bid rows are never updated and never need their own row
lock. `auction_id`/`bidder_id` reference `auctions(id)`/`users(id)` without
cascading deletes. `amount_minor` is a minor-unit `bigint` and must be
positive. The unique index on `(auction_id, bidder_id, idempotency_key)`
scopes client-supplied idempotency-key replay detection per bidder per
auction and doubles as its lookup index; because the acceptance transaction
locks the parent `auctions` row before ever consulting this index, concurrent
duplicate submissions are already serialized by that lock, so the constraint
is a defensive invariant rather than the primary concurrency guard.
`(auction_id, created_at DESC, id DESC)` supports a future keyset-paginated
bid-history read, mirroring `auctions_created_at_id_idx`. `auctions.leading_bid_id`
is a nullable reference to `bids`, added after `bids` exists so the two tables'
foreign keys point in only one direction inside the migration. It
denormalizes the current highest accepted bid so acceptance — and a future
cancellation check — are both a single locked read of the `auctions` row, as
documented in [`../../docs/AUCTION_LIFECYCLE.md`](../../docs/AUCTION_LIFECYCLE.md).
The foreign key is composite, `(id, leading_bid_id)` referencing
`bids (auction_id, id)`, backed by the otherwise-redundant
`bids_auction_id_id_key` unique constraint. A plain reference to `bids(id)`
would let an auction point at another auction's bid and leave that invariant
to writer discipline; the composite key makes PostgreSQL reject it.

The auction-owned outbox migration adds `outbox_events`. Each row stores the
immutable event JSON together with queryable event, auction, version, and
occurrence metadata; constraints keep that metadata aligned with the payload.
An update trigger makes the event identity, payload, and ordering metadata
append-only while still allowing a publisher to set `published_at`.
The unique auction/event/version constraint is a defensive guard against
duplicate domain events. The partial `(occurred_at, event_id)` index contains
only unpublished rows, allowing future concurrent publishers to claim ordered
batches with `FOR UPDATE SKIP LOCKED` without scanning published history.
`published_at` is nullable until the event is published.

The outbox publication-state migration adds the publisher's retry bookkeeping:
`publish_attempts` (non-negative), `next_publish_attempt_at` (null means "due
now"), and a bounded `last_publish_error`. These fields are not part of the
immutable event record, so the immutability trigger allows updating them. A
check constraint forbids a scheduled retry on a published event. The partial
`(auction_id, auction_version)` index on unpublished rows answers
the claim query's "no earlier unpublished event for this auction" check, which
provides per-auction head-of-line ordering. The claim query and its reasoning
are documented in
[`../../docs/OUTBOX_PUBLISHING.md`](../../docs/OUTBOX_PUBLISHING.md).

The outbox auction-sequence migration moves the ordering invariant that the
publisher relies on from writer discipline into the schema. The event key
becomes `(auction_id, auction_version)` instead of
`(auction_id, event_type, auction_version)`: two event types at one version
would have no defined publication order. A `BEFORE INSERT` trigger locks the
auction row with `FOR NO KEY UPDATE` and rejects an event whose version is not
the auction's current version. Any outbox writer must therefore bump the
auction version in its own transaction and hold the auction lock until commit,
so an auction's events become visible in version order. Bid acceptance already
holds a stronger `FOR UPDATE` lock, so the trigger's lock is a no-op there.
`FOR NO KEY UPDATE` does not conflict with the `FOR KEY SHARE` locks that
foreign-key checks take, so inserting bids is not blocked. The trigger checks
only new inserts; rows existing before the migration are not re-validated.

The contiguous-version migration enforces the converse of the outbox
sequence rule: every change to `auctions.version` must be a single step, and
must commit together with an outbox event for the new version. A `BEFORE
UPDATE OF version` trigger rejects any other step immediately. A `DEFERRABLE
INITIALLY DEFERRED` constraint trigger checks for the event at commit, because
a writer bumps the version first and only then may append the event for it.
Consumers can then treat a version gap as a lost event. Updates that leave the
version unchanged need no event, and existing rows are not re-validated.

Existing rows predate required title, currency, and bid-increment fields. The
migration derives their title from the description and backfills `USD`, a
one-minor-unit increment, version 1, and `updated_at = created_at`. Those
migration-only values are not column defaults for new auction creation.

Dbmate applies each migration in a transaction by default and records completed
versions in `schema_migrations`. Review both the migration and the resulting
generated `schema.sql` diff: the migration explains how the change happens, and
the snapshot shows the complete schema after it happens.

The identity refresh-session migration is owned by `identity-service` and adds:

- `identity_sessions`, with an absolute database-timestamp expiry, revocation
  state, timestamp-order checks, a user foreign key, and indexes for user lookup,
  expiry cleanup, and revoked-session cleanup;
- `identity_refresh_tokens`, with one 32-byte SHA-256 digest per unique token,
  unique generation numbers within a session, a single replacement child for
  each retained parent, timestamp checks, and a cascading session foreign key.
  The `(session_id, generation)` uniqueness index also supports family lookup;
- a digest uniqueness constraint that also supplies the indexed refresh lookup
  used under `FOR UPDATE` during rotation.

Identity owns all writes to these tables. Expired or sufficiently old revoked
sessions can be deleted in bounded batches; token history cascades only when its
parent session is deleted. Current and consumed token generations must remain
available while a session family is relevant so replay can be detected.

The PostgreSQL 18 Docker image uses the `postgres18_data` Compose volume mounted
at `/var/lib/postgresql` for persistent storage.

The finalization-failure migration adds three retry-bookkeeping columns to
`auctions` (see [`FINALIZATION.md`](../../docs/FINALIZATION.md#failed-closes)).
Apply it before starting a finalizer that records failures; older finalizers
keep working against the new schema.

The two settlement migrations are owned by `settlement-service` (see
[`SETTLEMENT.md`](../../docs/SETTLEMENT.md)). `settlement_processed_events`
records each event the worker has handled, keyed by `event_id`, so a redelivered
event is recognised. `orders` holds one row per sold auction and is guarded by
the database itself: `UNIQUE (auction_id)`, a composite foreign key to the
winning bid, and a `BEFORE INSERT` trigger that requires a closed auction and
checks the order's winning bid, buyer, price and currency against the auction's
recorded outcome (`check_violation` with a named constraint, in the style of
`auction_require_winner_on_close`). A second trigger rejects any change to an
order's facts. The settlement worker reads `auctions` and `bids` only through
that foreign key and trigger; apply both migrations before starting it. Rolling
back drops both tables, and with them every order.
