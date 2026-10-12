# Auction Bonanza Agent Guide

## Project context

- Auction Bonanza is a learning-focused, real-time auction backend.
- Read `README.md` for the product overview and `ACCEPTANCE.md` for the long-term failure and scale scenarios.
- The goal is not only to make the system work, but to make its Kafka, PostgreSQL, consistency, and recovery behavior understandable and defensible.

## Repository and service boundaries

- Use a monorepo, but design the system as multiple services rather than one deployable monolith.
- Choose pragmatic service boundaries based on behavior and ownership; do not follow microservice patterns dogmatically.
- Keep every service independently buildable, testable, deployable, and supplied with its own runtime configuration where practical.
- Use one root `docker-compose.yml` to run the local services and their infrastructure dependencies.
- Keep shared packages narrow and purpose-specific. Do not create a broad `libs/common/` dumping ground or move service-owned domain logic into shared code.
- Start with a single PostgreSQL database named `auction_db`, including the initial `users`, `auctions`, and `bids` tables. Do not force database-per-service prematurely.
- Make service and data ownership increasingly explicit as the system evolves. Treat migrations toward stronger ownership boundaries as deliberate, observable learning exercises.

## Working approach

- Build incrementally; avoid infrastructure or abstractions that the current milestone does not need.
- Preserve existing user changes and keep each change focused on the requested scope.
- State important assumptions and explain meaningful architecture tradeoffs.
- Prefer explicit behavior over hidden framework magic, especially around transactions, retries, ordering, and failure recovery.
- Record durable architectural decisions in project documentation when they become concrete.
- Place private methods at the end of a class, after its public methods.

## Code style

- Code that manages state belongs in a class that owns the state privately, not in functions that take and return it. A method that decides what to do returns the effects as data for the caller to carry out, rather than calling back into the caller. Pure functions are fine for stateless decisions.
- Keep a module's types next to it in `<module>.types.ts`. Types derived from a schema stay in the file that defines the schema.
- Never write one-line control statements such as `for (...) doThing();` or `if (x) return;`. Put the body on the next line inside `{ }`. The `curly` lint rule enforces this in `services/realtime-gateway` and `tests/e2e`; follow it in new code elsewhere too.
- Do not call a function that returns a value directly in an `if` condition unless it is a checker or validator (`has`, `startsWith`, `isStale`, `Value.Check`). Assign the result to a named variable first, for example `const outcome = this.applyEvent(event); if (outcome === "gap") {`. This is not lint-checked.

## Correctness principles

- Treat message delivery as potentially duplicated and design handlers to be idempotent where required.
- Protect auction, winner, order, and payment invariants with database constraints and transactions where practical.
- Make transaction boundaries and message acknowledgement boundaries explicit.
- Never use floating-point arithmetic for money.
- Include failure-path and concurrency tests when changing behavior that depends on retries, ordering, or atomicity.

## Validation

- Run the relevant tests, type checks, and lint commands exposed by the repository after making changes.
- Report what was verified and clearly identify checks that could not be run.
- Do not weaken tests merely to make a change pass.

## Collaboration

- Use concise explanations, but include enough reasoning to support the project's learning goals.
- Do not introduce custom agent roles by default. Use specialized or parallel agents only when explicitly requested or when a later project instruction calls for them.
