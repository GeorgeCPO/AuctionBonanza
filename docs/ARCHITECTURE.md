# Initial Architecture

Status: accepted as the initial direction; expected to evolve through deliberate exercises.

## Goals

- Keep all services in one Git repository without turning them into one deployable monolith.
- Make every service independently buildable, testable, deployable, and configurable.
- Prefer pragmatic boundaries over microservice dogma.
- Make transaction, messaging, ownership, and recovery behavior explicit enough to explain and test.

## Initial deployment units

| Service            | Initial responsibility                                                                                                                          |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `identity-service` | User registration, authentication, and user-owned data.                                                                                         |
| `auction-service`  | Auction lifecycle and synchronous bid acceptance. Auctions and bids remain together initially because they share a strong correctness boundary. |
| `realtime-gateway` | Consume published auction events and stream live updates to clients. It does not decide whether a bid is valid.                                 |

A settlement worker can be added when orders, payments, and auction closing enter scope.

## Request and event flow

```text
Client
  |-- HTTP commands and queries --> identity-service / auction-service
  |                                      |
  |                                      | PostgreSQL transaction
  |                                      v
  |                                  auction_db
  |                                      |
  |                                      | transactional outbox
  |                                      v
  |                                    Kafka
  |                                      |
  `-- SSE live updates <-- realtime-gateway
```

Bid acceptance is a synchronous operation in `auction-service`. For an
accepted bid, one database transaction validates the auction, persists the bid
and resulting auction state, and appends its outbox event atomically. A
separate outbox publisher worker, owned by `auction-service`, sends committed
events to Kafka. Kafka propagates committed outcomes to other services; it is
not used as request-response transport. Publication is at-least-once, with
per-auction ordering enforced by the publisher's claim query. See
[`OUTBOX_PUBLISHING.md`](OUTBOX_PUBLISHING.md) for the exact guarantees and
failure behavior.

## Technology choices

### Runtime and monorepo

- Use Node.js 24.20.0, TypeScript in strict mode, and ECMAScript modules. The version is pinned locally and in CI to align with the Node.js 24 AWS runtime family.
- Use `pnpm` workspaces for dependency and workspace management.
- Pin direct dependency and package-manager versions exactly. Commit `pnpm-lock.yaml` so all transitive dependency versions are reproducible; update dependencies deliberately as reviewed changes.
- Start with package scripts for per-service builds and tests. Add Turborepo only when task orchestration or build times justify it.
- Give each service its own package manifest, TypeScript configuration, Dockerfile, tests, and validated runtime configuration when implementation begins.

### HTTP APIs

- Use Fastify 5 for HTTP services.
- Use TypeBox and JSON Schema for request, response, and configuration validation.
- Generate an OpenAPI description with the Fastify Swagger plugins.
- Use REST/JSON for public APIs and initial synchronous service-to-service calls.
- Defer gRPC until a concrete need appears, such as cross-language contracts, streaming RPC, or measured request-response overhead.

### PostgreSQL

- Use `pg` (`node-postgres`) directly and write parameterized SQL.
- Keep transaction handling explicit and execute a transaction through one checked-out client.
- Use `dbmate` with raw SQL migration files.
- Treat SQL migrations as the executable source of truth. Commit the generated
  `db/schema.sql` snapshot so the complete current PostgreSQL schema is easy to
  inspect without reconstructing it from migration history.
- Keep PostgreSQL `bigint` IDs and monetary minor-unit values out of JavaScript `number`; use safe string or `bigint` representations and serialize them deliberately.
- Plan to deploy Amazon RDS for PostgreSQL 18.6. Local Compose uses the matching `postgres:18.6-bookworm` engine image; AWS-managed storage, backups, and failover remain intentional differences to document and test when deployment begins.

The first phase uses one PostgreSQL database, `auction_db`, with the initial `users`, `auctions`, and `bids` tables. Database-per-service is not required at the start.

Even while the database is shared, table writes should have a clear owning service. Moving toward stronger schema or database ownership later is part of the intended learning path and must be performed without unnecessary downtime.

### Kafka

- Run Apache Kafka in KRaft mode locally.
- Use the promisified API from `@confluentinc/kafka-javascript`.
- Use Kafka for asynchronous domain events, projections, notifications, settlement workflows, and replay.
- Assume messages may be delivered more than once. Consumers must make required side effects idempotent.
- Use a transactional outbox for PostgreSQL-to-Kafka publication. Add
  processed-event records when consumers begin updating PostgreSQL.
- Give events stable metadata. Current JSON contracts use `eventId`, a
  versioned `eventType` such as `BidAccepted.v1`, `auctionId` as the aggregate
  identifier, `auctionVersion` as its ordered sequence, and `occurredAt`.
- Initially key auction-related events by `auction_id` so ordering is preserved within an auction. Hot-auction behavior will be measured and revisited deliberately.
- Publish all auction events to one topic, `auction.events`, so different event types of one auction share a partition and a single order. Topics are provisioned explicitly; automatic topic creation is disabled.
- Hold outbox row locks through Kafka publication and mark events published only after an `acks=all` acknowledgement. This trades one PostgreSQL connection per publication loop for clean normal-case ordering; the lease-based alternative is discussed in [`OUTBOX_PUBLISHING.md`](OUTBOX_PUBLISHING.md).

Start with versioned JSON event schemas in `packages/contracts`. A schema registry and another encoding such as Avro or Protobuf can be introduced later as a separate compatibility exercise.

### Client live updates

- Submit bids through idempotent HTTP requests.
- Use Server-Sent Events for initial server-to-client auction updates.
- Multiplex subscriptions over a small number of client connections rather than opening one connection per auction.
- Include an auction version or sequence in updates. On reconnection, retrieve a current snapshot before applying newer events.
- Adopt WebSockets only when bidirectional messaging or another demonstrated requirement makes SSE insufficient.

### Logging, observability, and tests

- Use Fastify's Pino integration for structured logs.
- Propagate request, correlation, causation, event, and auction identifiers.
- Add OpenTelemetry traces and metrics as cross-service flows appear.
- Use Oxlint with its TypeScript 7 type-aware runtime for linting and Oxfmt for formatting. Keep `tsc --noEmit` as the canonical type check because Oxlint's type-check mode is still experimental.
- Use Vitest for test execution.
- Run integration tests against real PostgreSQL and Kafka instances from the root `docker-compose.yml`, both locally and in CI (see below).
- Use k6 for load generation and Toxiproxy for controlled network failures.

### Continuous integration

GitHub Actions runs one job that starts PostgreSQL and Kafka with the root
`docker-compose.yml`, applies migrations, checks `db/schema.sql` for drift, and
runs `pnpm check` with every integration test enabled. CI and local
development therefore share one infrastructure definition: image versions,
Kafka listeners (including the test-only `FAULT_INJECTION` listener), topic
provisioning, and health checks cannot drift apart. The services are
redefined neither as GitHub service containers nor in test code.

Integration tests skip when their `TEST_*` variables are unset so that
`pnpm check` stays usable without infrastructure. In CI a skip would pass
silently, so the service Vitest configurations fail the run when `CI=true` and
a variable is missing. This checks the known variables, not the outcome: a
test gated on a new variable, or an unconditional `.skip`, would not be caught
until its variable is added to the list. Asserting zero skipped tests would
catch both, but needs a custom reporter that sets the exit code, and forbids
any deliberate skip in CI.

Testcontainers is deferred. The current tests need one PostgreSQL and one
Kafka broker that every test file can share, which Compose already provides
and which developers already run. Testcontainers becomes worthwhile when tests
need infrastructure they can create, break, and discard per test: for example
a multi-broker cluster whose brokers are stopped to test leader election,
`min.insync.replicas`, or partition unavailability, or several PostgreSQL
versions side by side. Adopting it then also means reconciling it with the
Compose definition so the two do not drift.

## Deliberately deferred

- Testcontainers (see [Continuous integration](#continuous-integration))
- Redis or another cache as part of correctness-critical state
- gRPC
- Kafka Schema Registry
- Kubernetes
- Database-per-service
- A broad shared `common` package
- A monorepo task runner beyond workspace scripts

These are not prohibited. They should be introduced when a concrete milestone creates a reason to evaluate them.

## Authentication and availability

Identity signs minimal 10-minute access JWTs with ES256 and publishes public
keys as JWKS. Backend services use the narrow `packages/access-auth` verifier,
pin algorithm/type/issuer/audience and claims, and cache at most eight public
keys for a configured TTL. A normal request with a known cached key performs no
identity-service or database call. An unknown `kid` refreshes JWKS, enabling old
and new keys to overlap during rotation.

Opaque 256-bit refresh credentials go only to identity. PostgreSQL stores their
SHA-256 digests in retained, linked generations beneath a stable refresh
session. Rotation is a single explicit transaction: lock token and session,
validate against database time, consume, insert replacement, update session,
commit, then acknowledge over HTTP. Reuse of a consumed generation revokes the
entire family. Kafka and Redis are not involved.

| Failure or transition                                    | Behavior                                                                                              |
| -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Identity unavailable; backend has token/fresh cached key | Backend verification continues locally.                                                               |
| Identity unavailable during login or refresh             | Operation fails; no backend fallback creates credentials.                                             |
| PostgreSQL fails before refresh commit                   | Transaction rolls back; the old refresh token remains current.                                        |
| Refresh commits but HTTP response is lost                | Retrying the old token looks like replay, revokes the family, and requires login.                     |
| Same refresh token submitted concurrently                | Row locks serialize uses; one may succeed, the other detects replay and revokes the family.           |
| Rotated refresh token replayed                           | Entire session family is revoked; caller receives only a generic `401`.                               |
| User logs out with a still-valid access token            | Refresh stops immediately; access remains valid for at most 10 minutes plus 5 seconds skew.           |
| Signing key rotates while old access tokens remain valid | JWKS publishes both keys until old token lifetime, skew, and cache propagation have elapsed.          |
| Service cold-starts without JWKS while identity is down  | Authentication fails closed until JWKS can be retrieved (or a future secure bootstrap is configured). |
| Service receives a token for another audience            | Strict verification rejects it with `401`.                                                            |

Access tokens represent initiating-user context only. They are not credentials
for the authenticated calling service. Future internal authentication may use
mTLS, workload credentials, or token exchange, and must preserve that
distinction without trusting client-set internal headers.
