# Initial Architecture

Status: accepted as the initial direction; expected to evolve through deliberate exercises.

## Goals

- Keep all services in one Git repository without turning them into one deployable monolith.
- Make every service independently buildable, testable, deployable, and configurable.
- Prefer pragmatic boundaries over microservice dogma.
- Make transaction, messaging, ownership, and recovery behavior explicit enough to explain and test.

## Initial deployment units

| Service | Initial responsibility |
| --- | --- |
| `identity-service` | User registration, authentication, and user-owned data. |
| `auction-service` | Auction lifecycle and synchronous bid acceptance. Auctions and bids remain together initially because they share a strong correctness boundary. |
| `realtime-gateway` | Consume published auction events and stream live updates to clients. It does not decide whether a bid is valid. |

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

Bid acceptance starts as a synchronous operation in `auction-service`. The database transaction will eventually validate the auction, persist the accepted bid and resulting state, and write an outbox event atomically. Kafka propagates committed outcomes to other services; it is not initially used as request-response transport.

## Technology choices

### Runtime and monorepo

- Use the current Node.js 24 LTS release, TypeScript in strict mode, and ECMAScript modules.
- Use `pnpm` workspaces for dependency and workspace management.
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
- Treat SQL migrations as the executable source of truth. Keep `db/schema.dbml` as the human-readable design model.
- Keep PostgreSQL `bigint` IDs and monetary minor-unit values out of JavaScript `number`; use safe string or `bigint` representations and serialize them deliberately.

The first phase uses one PostgreSQL database, `auction_db`, with the initial `users`, `auctions`, and `bids` tables. Database-per-service is not required at the start.

Even while the database is shared, table writes should have a clear owning service. Moving toward stronger schema or database ownership later is part of the intended learning path and must be performed without unnecessary downtime.

### Kafka

- Run Apache Kafka in KRaft mode locally.
- Use the promisified API from `@confluentinc/kafka-javascript`.
- Use Kafka for asynchronous domain events, projections, notifications, settlement workflows, and replay.
- Assume messages may be delivered more than once. Consumers must make required side effects idempotent.
- Introduce a transactional outbox for PostgreSQL-to-Kafka publication and processed-event records for consumers that update PostgreSQL.
- Give events stable metadata including `event_id`, `event_type`, `event_version`, `aggregate_id`, and `occurred_at`.
- Initially key auction-related events by `auction_id` so ordering is preserved within an auction. Hot-auction behavior will be measured and revisited deliberately.

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
- Use Vitest for test execution.
- Use Testcontainers for integration tests against real PostgreSQL and Kafka instances.
- Use k6 for load generation and Toxiproxy for controlled network failures.

## Deliberately deferred

- Redis or another cache as part of correctness-critical state
- gRPC
- Kafka Schema Registry
- Kubernetes
- Database-per-service
- A broad shared `common` package
- A monorepo task runner beyond workspace scripts

These are not prohibited. They should be introduced when a concrete milestone creates a reason to evaluate them.
