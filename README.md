# Auction Bonanza

Real-time auction platform

Users can create auctions, place bids, watch prices update live, use automatic/max bidding, and eventually settle an auction into an order/payment.

Then add load-generating bots so you're dealing with millions of auctions and hundreds of millions of bids rather than your own five test users.

## Description

Develop a robust auction backend that will confront "distributed system" problems.

This is a project aimed to turn a competent backend engineer into someone who can confidently discuss Kafka partition strategy, database internals, distributed consistency, SQL optimization, migrations and AWS architecture in a senior-level interview.

## Tech stack

- Node.js
- TypeScript
- PostgreSQL
- Kafka
- AWS stack
- Open for extension

## Local foundation

Prerequisites are Node.js 24.20.0, pnpm 11.25.0, Docker with Compose, and
PostgreSQL client tools containing `pg_dump` 18.x for schema snapshots.

Direct dependencies are pinned to exact versions. Use `pnpm add --save-exact`
when adding a dependency (the repository enforces this default), and commit the
resulting `pnpm-lock.yaml` change.

```sh
pnpm install
pnpm check
docker compose up -d postgres
cp .env.example .env
pnpm db:migrate
pnpm auth:keys:local local-2026-09
```

Use `pnpm format` to apply repository formatting, `pnpm format:check` to verify
it without writing files, and `pnpm lint` for type-aware linting. `pnpm check`
runs formatting, linting, type checks, tests, and production builds.

The Compose stack currently contains PostgreSQL only. It runs
`postgres:18.6-bookworm` to match the planned Amazon RDS for PostgreSQL 18.6
engine version, and exposes the local `auction_db` database on port `5432`
using the development-only credentials in `docker-compose.yml`.

Database migrations are owned centrally because the initial services share
`auction_db`. Use `pnpm db:status`, `pnpm db:migrate`, and `pnpm db:rollback`
from the repository root. Migrate and rollback regenerate the committed
`db/schema.sql` snapshot. See `db/migrations/README.md` for the workflow, naming,
and schema decisions.

PostgreSQL 18 uses the versioned `postgres18_data` Compose volume mounted at
`/var/lib/postgresql`, matching the official image's persistent-storage layout.

To exercise the identity service's runtime configuration loader:

```sh
cp services/identity-service/.env.example services/identity-service/.env
pnpm --filter @auction-bonanza/identity-service build
pnpm --filter @auction-bonanza/identity-service start:local
```

The process validates all configuration before service startup. The `.env` file
is ignored by Git; `.env.example` is the committed local-development contract.
Signing keys under `services/identity-service/.local-keys` are also ignored and
are for local development only.
