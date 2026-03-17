# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AuctionBonanza is a learning project building a robust auction backend to explore distributed system challenges. The stack is Node.js + Fastify + TypeScript + PostgreSQL + postgres.js (raw SQL, no ORM).

## Commands

No build/test/lint tooling has been configured yet. As infrastructure is added, update this section with:
- How to start the dev server
- How to run tests (single test and full suite)
- How to run linting/type checking
- How to run database migrations

## Architecture

AuctionBonanza is a **modular monolith** structured so each module can be extracted into its own microservice with minimal refactoring.

### Folder Structure

```
src/
├── index.ts                    # Entry point: boots Fastify
├── app.ts                      # App factory: wires shared infra + registers modules
├── shared/                     # Infrastructure owned by no single module
│   ├── db/
│   │   ├── client.ts           # postgres.js pool singleton
│   │   └── migrations/         # Numbered SQL migration files
│   ├── events/
│   │   ├── bus.ts              # In-process EventEmitter wrapper
│   │   └── events.ts           # All event name constants (single source of truth)
│   ├── errors/
│   │   └── http-errors.ts      # Typed HTTP error classes
│   ├── config/
│   │   └── index.ts            # Reads + validates process.env, exports typed config
│   └── types/
│       └── fastify.d.ts        # Fastify type augmentations
└── modules/
    ├── auth/                   # Session management, login/logout
    ├── users/                  # Registration, role management
    ├── auctions/               # Create/start/end auctions, state machine
    └── items/                  # Heterogeneous item types (laptop, phone, car, etc.)

tests/
└── modules/                    # Mirrors src/modules/ structure
```

Each module contains: `index.ts` (public API / Fastify plugin), `*.routes.ts`, `*.service.ts`, `*.repository.ts`, `*.schemas.ts`, `*.types.ts`.

### Module Boundary Rules

1. **No cross-module internal imports** — only import from another module's `index.ts`, never its internal files.
2. **No cross-module SQL joins** — each module owns its own tables exclusively.
3. **Events are the only cross-module write path** — emit an event on the shared bus; never call another module's service directly for writes.
4. **Shared infra is injected** — modules receive the DB client and event bus via Fastify plugin options, not direct imports from `shared/`.

### Cross-Module Communication

- **Reads:** Define a typed interface in the consuming module (e.g. `UserLookupService`), inject an adapter at startup in `app.ts`. Swap to HTTP client when extracting to a service.
- **Writes/side-effects:** In-process event bus (`shared/events/bus.ts`). Replace with Kafka/RabbitMQ when extracting. Event payloads must be self-contained.

### Fastify Conventions

- Each `modules/*/index.ts` is a `fastify-plugin`-wrapped async function.
- Route URL prefixes are set in `app.ts` at registration time, not inside modules.
- JSON Schema defined in `*.schemas.ts` and attached to every route (required for Fastify serialization performance).
- Auth decorates `request.session`/`request.user`; other modules use `fastify.verifySession` as a `preHandler` hook.

### Planned Domain

- **Auth:** Email/password login with session cookies
- **Users:** Registration and role management (regular user → auctioneer)
- **Auctions:** Auctioneers create and manage auctions; state machine (pending → active → ended)
- **Items:** Heterogeneous types (laptops, phones, cars) — item type strategy TBD (JSONB vs child tables)

### Known Design Decisions Pending

- **Auction state transitions** must use atomic SQL (`UPDATE ... WHERE status = 'pending'`) to avoid race conditions — never read-then-write in the service layer.
- **Event bus durability:** Current in-process bus is fire-and-forget. Must be replaced with a transactional outbox or durable broker at scale.

### Intentional Simplifications

- **Items** are placeholder data only: `name`, `description`, `starting_price`, `item_type` (string label). No type-specific schemas or queries. The focus of this project is live auction mechanics and real-time updates at scale — not item catalog complexity.
- **Payments** are mocked. No real payment processing.

### Workflow diagrams -- TO DO STILL IN PROGRESS

User flows are documented in `workflow_diagrams/` as Mermaid diagrams:
- `login.md` — Email/password auth, returns session cookie on success
- `register.md` — User registration (empty, needs filling out)
- `become_auctioneer.md` — Role escalation flow (incomplete)
- `auctioneer.md` — Create auction → start → end with item type branching

## Conventions

- Do not proactively refactor code; prefer existing patterns when extending.
- Use the `gh` CLI for all GitHub operations (PRs, issues, checks, releases).
