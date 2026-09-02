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

Dbmate applies each migration in a transaction by default and records completed
versions in `schema_migrations`. Review both the migration and the resulting
generated `schema.sql` diff: the migration explains how the change happens, and
the snapshot shows the complete schema after it happens.
