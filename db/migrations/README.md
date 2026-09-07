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
stale snapshot when the client tool is missing.

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
