# Testing

Tests come in three tiers. Each has its own folder and its own command, and what
a test depends on decides its tier.

| Tier        | Lives in                        | May depend on                                                                                       | Must not depend on                         |
| ----------- | ------------------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| Unit        | `<workspace>/test/unit/`        | In-memory fakes, fake timers, loopback sockets, Fastify `inject`                                    | PostgreSQL, Kafka, Valkey, child processes |
| Integration | `<workspace>/test/integration/` | One service's own code against real PostgreSQL, Kafka or Valkey; may start that service's processes | Another service's code                     |
| End-to-end  | `tests/e2e/test/`               | Every service as a black box, over HTTP, the database and the infrastructure                        | Importing any service's code               |

`<workspace>/test/support/` holds helpers shared by the tiers of one workspace
(for example the realtime gateway's `edge-harness`, which unit and integration
tests both use). Integration files keep the `.integration.test.ts` suffix so a
file name still says its tier in logs, stack traces and editor tabs.

## Commands

Run from the repository root. Every one also exists inside a workspace, where it
runs only that workspace (`pnpm --filter @auction-bonanza/auction-service test:unit`).
Pass a file after the command to run just that file, with no `--` in between.
There is deliberately no bare `test` script inside a workspace, so a
workspace-level run always names its tier.

| Command                          | What it does                                                                        | Needs Docker |
| -------------------------------- | ----------------------------------------------------------------------------------- | ------------ |
| `pnpm test`                      | Unit tests (same as `test:unit`)                                                    | No           |
| `pnpm test:unit`                 | Unit tests of every workspace                                                       | No           |
| `pnpm test:unit:coverage`        | Unit tests with coverage; fails below each workspace's thresholds                   | No           |
| `pnpm test:integration`          | Integration tests; needs the `TEST_*` variables                                     | Yes          |
| `pnpm test:integration:local`    | `test:integration` with the Compose endpoints from `scripts/local-test.env`         | Yes          |
| `pnpm test:integration:coverage` | Integration tests with an informational coverage report (see below)                 | Yes          |
| `pnpm test:e2e`                  | The end-to-end suite; needs the `TEST_*` variables                                  | Yes          |
| `pnpm test:e2e:local`            | `test:e2e` with the Compose endpoints from `scripts/local-test.env`                 | Yes          |
| `pnpm check`                     | Format check, lint, typecheck, `test:unit:coverage`, build. Needs no infrastructure | No           |
| `pnpm check:all`                 | `check`, then `test:integration`, then `test:e2e`: everything CI runs               | Yes          |
| `pnpm check:all:local`           | `check:all` with `CI=true` and the local endpoints: reproduces the CI run           | Yes          |
| `pnpm test:sim:replay <seed>`    | Replays one seed of the realtime gateway's registry simulation                      | No           |

Inside a workspace there is also `test:unit:watch` (Vitest in watch mode), which
only makes sense for one workspace at a time.

## Running against local infrastructure

```sh
docker compose up --detach --wait postgres kafka valkey-1 valkey-2 valkey-3 \
  valkey-topology-1 valkey-topology-2 valkey-topology-3 \
  valkey-topology-4 valkey-topology-5 valkey-topology-6
docker compose up kafka-init valkey-init valkey-topology-init
pnpm test:integration:local
pnpm test:e2e:local
```

`scripts/local-test.env` holds the development-only endpoints of that stack
(the same values as `docker-compose.yml`). The `:local` scripts run through
`scripts/with-local-test-env.mjs`, which loads it without overriding variables
already set in your shell, so one value can still be changed for a single run:

```sh
TEST_KAFKA_BROKERS=localhost:9093 pnpm test:integration:local
```

CI never loads that file. `.github/workflows/ci.yml` sets its own values, so a
variable missing there fails the run instead of being filled in.

Variables each tier needs:

| Workspace            | Variables                                                                        |
| -------------------- | -------------------------------------------------------------------------------- |
| `auction-service`    | `TEST_DATABASE_URL`, `TEST_KAFKA_BROKERS`, `TEST_KAFKA_FAULT_INJECTION_UPSTREAM` |
| `identity-service`   | `TEST_DATABASE_URL`                                                              |
| `realtime-gateway`   | `TEST_KAFKA_BROKERS`, `TEST_VALKEY_NODES`, `TEST_VALKEY_TOPOLOGY_NODES`          |
| `settlement-service` | `TEST_DATABASE_URL`, `TEST_KAFKA_BROKERS`                                        |
| `tests/e2e`          | `TEST_DATABASE_URL`, `TEST_KAFKA_BROKERS`, `TEST_VALKEY_NODES`                   |

The outbox publisher, finalizer, settlement and end-to-end tests create and drop
their own temporary databases from `db/schema.sql` (the database user needs
`CREATEDB`), and their own Kafka topics. The Kafka outage test runs a TCP proxy on port
`19092`, which must be free; it reaches the broker through the test-only
`FAULT_INJECTION` listener on `19093`.

## The Go load tool

`tools/loadgen` has its own gate, because `pnpm check` is Node-only and needs no
Go toolchain. It follows the same tier rules: plain `go test` files are the unit
tier, and files with `//go:build integration` use real PostgreSQL and Kafka.

| Command                               | What it does                                                                                          | Needs Docker |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------- | ------------ |
| `pnpm loadgen:check`                  | `gofmt` (fails on unformatted files), `go vet`, vet of the integration files, unit tests with `-race` | No           |
| `pnpm loadgen:test:integration`       | Integration tests; needs `TEST_DATABASE_URL` and `TEST_KAFKA_BROKERS`                                 | Yes          |
| `pnpm loadgen:test:integration:local` | The same with the Compose endpoints from `scripts/local-test.env`                                     | Yes          |

A missing variable fails the run, as in the other tiers. The tagged run also
executes the unit tests of the packages it covers. CI runs both commands after the
end-to-end tests. Go coverage is not measured or gated yet.

## Missing infrastructure fails the run

The integration and end-to-end Vitest configurations refuse to start when a
required variable is missing, and the error names it. Tests do not skip
themselves: a green `test:integration` or `test:e2e` means every test in that
tier ran. Running only part of a tier is a choice, made by passing a file or a
`-t` filter.

The unit configuration runs everything outside `test/integration/`. A test put
in the wrong folder therefore runs in the hermetic tier, where missing
infrastructure fails it, instead of never running.

## Coverage

Coverage uses the V8 provider (`@vitest/coverage-v8`, pinned to the Vitest
version). Reports go to `coverage/` in each workspace, which Git, the linter and
the formatter all ignore; open `coverage/unit/index.html` for a per-line view.

| Tier        | Measured             | Enforced                                                               |
| ----------- | -------------------- | ---------------------------------------------------------------------- |
| Unit        | Yes                  | Yes. Each workspace sets its own thresholds in `vitest.unit.config.ts` |
| Integration | In-process code only | No. `test:integration:coverage` is for reading, not gating             |
| End-to-end  | No                   | No                                                                     |

**Unit coverage is the gate.** Thresholds are per workspace, because each
service owns its own bar, and sit just below the measured value when they were
set. They exist to stop regressions, not to name a target. When coverage rises,
raise the threshold in the same change.

**Integration coverage is informational.** Tests that run service code inside
the Vitest process (repositories, schema checks, the Valkey and Kafka clients,
the gateway harness classes) are measured exactly like unit tests, so the report
shows which SQL and client paths only a real database or broker exercises. Tests
that start a service as a child process (the `*-process.integration.test.ts`
files) are not measured: the provider only instruments the Vitest process.
Merging child-process coverage would need `NODE_V8_COVERAGE`, c8 and source-map
remapping from the throwaway build directory. No threshold applies, because
these tests exist to prove atomicity, ordering and retry behaviour, which a line
count does not measure.

**End-to-end coverage is not measured.** The suite is a black box by design: it
compiles each service into a temporary directory and talks to the running
processes, and imports none of their code. Instrumenting that costs more than
the number is worth. Its coverage is the list of scenarios in the
[roadmap](../ACCEPTANCE.md).

There is no merged repository-wide report. Per-workspace reports and thresholds
match the service ownership model and keep each workspace testable on its own.

## Replaying the simulation

`services/realtime-gateway/test/unit/auction-registry.simulation.test.ts` runs
randomized worlds from fixed seeds. A failure prints its seed; replay just that
one with:

```sh
pnpm test:sim:replay 123
```
