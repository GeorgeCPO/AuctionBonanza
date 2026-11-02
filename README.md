# Auction Bonanza

A real-time auction backend, built to make Kafka, PostgreSQL, consistency and
failure-recovery behavior understandable and documented.

Users register, create auctions, place bids and watch prices update live. When an
auction ends it is closed, the winning bid is recorded and an order is created
for the winner.

The point of the project is not only that it works, but that the reasons it
keeps working are written down: what happens when a message is delivered twice,
a consumer crashes mid-message, or a broker disappears. [`ACCEPTANCE.md`](ACCEPTANCE.md)
is the roadmap of such failure and scale scenarios.

**Status:** registration and sign-in, auctions and idempotent bidding, live
updates over SSE, auction finalization, and order creation are implemented.
Automatic (max) bidding, payment and the AWS deployment are planned.

## Contents

- [How it works](#how-it-works)
- [Run it locally](#run-it-locally)
- [Repository layout](#repository-layout)
- [Development](#development)
- [Documentation](#documentation)

## How it works

The system is a handful of small services in one monorepo. They share one
PostgreSQL database (`auction_db`) and communicate through Kafka events.

```text
Browser (apps/web)
  |-- HTTP --> identity-service            sign-in, tokens
  |-- HTTP --> auction-service ----------> PostgreSQL (auction_db)
  |                 |                          |
  |                 |   bid + auction update + outbox row
  |                 |   commit in ONE transaction
  |                 v
  |          outbox publisher --> Kafka (auction.events)
  |                                  |            |
  |                                  |            `--> settlement-service --> orders (PostgreSQL)
  |                                  v
  |                           realtime relay --> Valkey pub/sub
  |                                                    |
  `<-- SSE: snapshot, then live bids -- realtime edge -'
```

| Component                     | Responsibility                                                                                                                          | Port |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ---- |
| `identity-service`            | Registration, sign-in, short-lived access tokens (ES256 JWT) and rotating single-use refresh tokens.                                    | 3001 |
| `auction-service` (API)       | Auction lifecycle and idempotent bid acceptance inside PostgreSQL transactions.                                                         | 3002 |
| `auction-service` (outbox)    | A separate worker that publishes `BidAccepted` and `AuctionClosed` events from the transactional outbox to Kafka, in order per auction. | n/a  |
| `auction-service` (finalizer) | A separate worker that closes ended auctions and records the winning bid (or no winner).                                                | n/a  |
| `settlement-service`          | Consumes `AuctionClosed` and creates exactly one order per sold auction. Unprocessable records go to a dead-letter topic. No HTTP API.  | n/a  |
| `realtime-gateway` (relay)    | Forwards Kafka events to Valkey, one sharded channel per auction.                                                                       | n/a  |
| `realtime-gateway` (edge)     | Serves Server-Sent Events: a snapshot per auction, then live `bid-accepted` events. Repairs gaps by resyncing from `auction-service`.   | 3003 |
| `apps/web`                    | A barebones React UI to sign in, watch prices move, bid and see the raw event log. A window onto the backend, not a product.            | 5173 |
| `tools/loadgen`               | A Go tool that seeds users and auctions and samples PostgreSQL and Kafka into CSV for the scale scenarios. Not a service.               | n/a  |

Principles the code is held to:

- **Delivery may be duplicated.** Handlers are idempotent; a consumer records
  each processed event in the same transaction as its effect, and acknowledges
  Kafka only after that commit.
- **Invariants live in the database.** Auction, winner and order rules are
  protected by constraints and transactions, not only by application code.
- **Money is never a float.** Amounts are decimal strings of minor units
  (cents) end to end.
- **Failure behavior is explicit.** Transaction, retry, ordering and
  acknowledgement boundaries are spelled out, and tested where they matter.

## Run it locally

Everything runs on your machine: the infrastructure in Docker, the services as
plain Node.js processes. There is no single "start everything" command yet; you
start the infrastructure once and then run the services, each in its own
terminal.

### Prerequisites

| Tool                | Version                  | Notes                                                                          |
| ------------------- | ------------------------ | ------------------------------------------------------------------------------ |
| Node.js             | 24.20.0 (see `.nvmrc`)   | The version is enforced (`engine-strict`). With nvm: `nvm install && nvm use`. |
| pnpm                | 11.25.0                  | Pinned in `package.json`. Run `corepack enable` and the right version is used. |
| Docker with Compose | Compose v2               | Runs PostgreSQL, Kafka and Valkey.                                             |
| Go                  | Only for `tools/loadgen` | Optional. Not needed to run or test the services.                              |

Free ports: `3001`, `3002`, `3003`, `5173`, `5432`, `9092` and `7001`–`7003`.

### 1. Install, configure and build

From the repository root:

```sh
pnpm install

# Local configuration. These files are git-ignored and ready to use as copied.
cp .env.example .env
for f in services/*/.env*.example; do cp -n "$f" "${f%.example}"; done

# Local signing key for access tokens (written to services/identity-service/.local-keys)
pnpm auth:keys:local

pnpm build
```

### 2. Start the infrastructure

```sh
docker compose up -d --wait postgres kafka kafka-init valkey-init
PATH="$PWD/scripts/compose-bin:$PATH" pnpm db:migrate
```

The first command starts PostgreSQL, a single-node Kafka (and creates its
topics) and a three-node Valkey cluster. The second creates the tables. Setting
`PATH` makes `pg_dump` run inside the Postgres container, so you don't need
PostgreSQL client tools installed. Details on every container are in
[`docs/LOCAL_INFRASTRUCTURE.md`](docs/LOCAL_INFRASTRUCTURE.md).

### 3. Start the services

Open one terminal per row, in this order (the auction service needs identity's
public keys, and the edge needs the auction service):

| #   | Process             | Command                                                                       | Listens on |
| --- | ------------------- | ----------------------------------------------------------------------------- | ---------- |
| 1   | identity-service    | `pnpm --filter @auction-bonanza/identity-service start:local`                 | 3001       |
| 2   | auction-service API | `pnpm --filter @auction-bonanza/auction-service start:local`                  | 3002       |
| 3   | outbox publisher    | `pnpm --filter @auction-bonanza/auction-service start:outbox-publisher:local` | n/a        |
| 4   | finalizer           | `pnpm --filter @auction-bonanza/auction-service start:finalizer:local`        | n/a        |
| 5   | settlement worker   | `pnpm --filter @auction-bonanza/settlement-service start:local`               | n/a        |
| 6   | realtime relay      | `pnpm --filter @auction-bonanza/realtime-gateway start:relay:local`           | n/a        |
| 7   | realtime edge       | `pnpm --filter @auction-bonanza/realtime-gateway start:edge:local`            | 3003       |
| 8   | web UI              | `pnpm web:dev`                                                                | 5173       |

Each process validates its configuration at startup and exits with a clear
message if something is missing. Logs are structured JSON. Stop a process with
Ctrl-C.

Check that the APIs are up:

```sh
curl localhost:3001/health   # {"status":"ok"}
curl localhost:3002/ready    # {"status":"ready"}
```

### 4. Try it

Open <http://localhost:5173>.

1. Register two users, one in each of two browser windows (tokens are kept in
   memory only, so a page reload signs you out).
2. As the first user, create an auction that ends in 2 minutes.
3. As the second user, place a bid. The price and leading bid update live in
   both windows.
4. Bid too low, or bid on your own auction, to see the rejection messages.
5. Wait for the deadline. The phase moves from `open` to `ended` and then
   `closed` once the finalizer runs, and settlement creates the order:

   ```sh
   docker compose exec postgres psql -U auction auction_db -c 'SELECT * FROM orders'
   ```

The same flow works without the UI. Watch an auction's live stream (use an
auction ID from the UI or from `curl localhost:3002/v1/auctions`):

```sh
curl -N 'http://localhost:3003/v1/stream?auctions=<id>'
```

Each stream starts with a `snapshot` per auction, then `bid-accepted` events.

To see messages and consumer-group lag in Kafka, start the optional Kafka UI
with `docker compose up -d kafka-ui` and open <http://localhost:8080>.

### 5. Stop and reset

```sh
docker compose down            # stops the infrastructure, keeps the data
docker compose down --volumes  # also deletes the database, Kafka data and Valkey state
```

### Troubleshooting

| Symptom                                               | Likely cause and fix                                                                         |
| ----------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `ERR_PNPM_UNSUPPORTED_ENGINE`                         | Wrong Node.js version. Use the one in `.nvmrc`.                                              |
| A service exits at startup naming a missing variable  | Its `.env*` file is missing. Rerun the `cp` loop in step 1.                                  |
| `Cannot find module .../dist/...`                     | Run `pnpm build` again.                                                                      |
| auction-service fails to verify tokens                | Identity-service isn't running, or you skipped `pnpm auth:keys:local`. Start identity first. |
| Settlement or the outbox publisher can't find a topic | Run `docker compose up kafka-init`.                                                          |
| `port is already allocated` / `EADDRINUSE`            | Something else uses that port. Stop it or stop the previous run.                             |
| `pg_dump: command not found` during `db:migrate`      | Prefix the command with `PATH="$PWD/scripts/compose-bin:$PATH"` as in step 2.                |

## Repository layout

```text
apps/web/              React UI (Vite)
services/
  identity-service/    users, sessions, JWKS
  auction-service/     API, outbox publisher and finalizer
  settlement-service/  Kafka consumer that creates orders
  realtime-gateway/    relay (Kafka to Valkey) and SSE edge
packages/              narrow shared packages (event contracts, token verification, HTTP routing)
tools/loadgen/         Go load and sampling tool
tests/e2e/             black-box tests across all services
db/                    SQL migrations and the committed schema snapshot
docs/                  design and behavior documentation
scripts/               local helpers (keys, test environment, pg_dump wrapper)
docker-compose.yml     the local infrastructure
```

Each service is independently buildable and testable and has its own `README.md`
and `.env*.example` files.

## Development

Run these from the repository root.

| Command                                         | What it does                                                                          | Needs Docker |
| ----------------------------------------------- | ------------------------------------------------------------------------------------- | ------------ |
| `pnpm check`                                    | Format check, lint, type check, unit tests with coverage thresholds, production build | No           |
| `pnpm test:unit`                                | Unit tests only                                                                       | No           |
| `pnpm test:integration:local`                   | Integration tests against real PostgreSQL, Kafka and Valkey                           | Yes          |
| `pnpm test:e2e:local`                           | End-to-end tests: every service as a black box                                        | Yes          |
| `pnpm check:all:local`                          | Everything above, as CI runs it                                                       | Yes          |
| `pnpm format` / `pnpm lint`                     | Apply formatting / run type-aware linting                                             | No           |
| `pnpm db:status` / `db:migrate` / `db:rollback` | Database migrations; migrate and rollback also refresh `db/schema.sql`                | Yes          |

The integration and end-to-end tiers need the full Compose stack, including the
second Valkey cluster used by topology tests:

```sh
docker compose up -d --wait
```

They refuse to start (rather than skip) if their `TEST_*` variables are missing;
the `:local` commands set them for you. Test tiers and their rules are in
[`docs/TESTING.md`](docs/TESTING.md).

Direct dependencies are pinned to exact versions. Use `pnpm add --save-exact`
when adding one, and commit the resulting `pnpm-lock.yaml` change.

GitHub Actions runs the same checks on every pull request and on pushes to
`master`: it brings up the Compose stack, verifies that `db/schema.sql` matches
the migrations, then runs `pnpm check`, the integration tests and the
end-to-end tests. How to reproduce that run locally is in
[`docs/LOCAL_INFRASTRUCTURE.md`](docs/LOCAL_INFRASTRUCTURE.md#continuous-integration).

## Documentation

| Document                                                       | Covers                                                                  |
| -------------------------------------------------------------- | ----------------------------------------------------------------------- |
| [`ACCEPTANCE.md`](ACCEPTANCE.md)                               | Roadmap: failure and scale scenarios the system should handle           |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)                 | Service boundaries, technology choices, authentication and availability |
| [`docs/AUCTION_LIFECYCLE.md`](docs/AUCTION_LIFECYCLE.md)       | Auction phases and the bid-acceptance contract                          |
| [`docs/OUTBOX_PUBLISHING.md`](docs/OUTBOX_PUBLISHING.md)       | How committed events reach Kafka; delivery and ordering guarantees      |
| [`docs/FINALIZATION.md`](docs/FINALIZATION.md)                 | How ended auctions are closed and winners recorded                      |
| [`docs/SETTLEMENT.md`](docs/SETTLEMENT.md)                     | Order creation, idempotency and the dead-letter topic                   |
| [`docs/REALTIME_GATEWAY.md`](docs/REALTIME_GATEWAY.md)         | Relay, edge, client protocol and scaling characteristics                |
| [`docs/FRONTEND.md`](docs/FRONTEND.md)                         | Web UI design decisions and known gaps                                  |
| [`docs/TESTING.md`](docs/TESTING.md)                           | Unit, integration and end-to-end tiers                                  |
| [`docs/LOCAL_INFRASTRUCTURE.md`](docs/LOCAL_INFRASTRUCTURE.md) | Compose services, Kafka topics, Valkey clusters, migrations, CI         |
| [`tools/loadgen/README.md`](tools/loadgen/README.md)           | Seeding and sampling for the scale scenarios                            |

## Tech stack

Node.js 24 and TypeScript (strict, ES modules), Fastify 5, PostgreSQL 18 (plain
SQL via `pg`, migrations with dbmate), Apache Kafka 4.3 in KRaft mode, Valkey 9
(cluster mode), React 19 with Vite, Go for the load tool, Vitest, Oxlint and
Oxfmt, Docker Compose and GitHub Actions. Local versions mirror the managed AWS
services planned for deployment (RDS for PostgreSQL, ElastiCache for Valkey).
