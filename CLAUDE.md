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

The project is in greenfield/planning phase. Source code goes in `src/` with the entry point at `src/index.js` (planned to be TypeScript). Tests go in `tests/`.

### Planned Domain

- **Auth:** Email/password login with session cookies
- **Users:** Registration and role management (regular user → auctioneer)
- **Auctions:** Auctioneers create and manage auctions; items include heterogeneous types (laptops, phones, cars, etc.)

### Workflow diagrams

User flows are documented in `workflow_diagrams/` as Mermaid diagrams:
- `login.md` — Email/password auth, returns session cookie on success
- `register.md` — User registration (empty, needs filling out)
- `become_auctioneer.md` — Role escalation flow (incomplete)
- `auctioneer.md` — Create auction → start → end with item type branching

## Conventions

- Do not proactively refactor code; prefer existing patterns when extending.
- Use the `gh` CLI for all GitHub operations (PRs, issues, checks, releases).
