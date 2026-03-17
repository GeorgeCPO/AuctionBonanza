---
name: Modular Monolith Architecture
description: AuctionBonanza is a modular monolith structured for future service extraction
type: project
---

AuctionBonanza is intentionally structured as a modular monolith where each module (auth, users, auctions, items) can be extracted into its own microservice with minimal code changes.

**Why:** The app will need to scale to millions of users in production, so the internal structure mirrors what a distributed system would look like — but ships as one deployable unit.

**How to apply:** When implementing any feature, enforce these rules:
- Only import from another module's `index.ts` (never internals)
- No cross-module SQL joins — each module owns its tables
- Cross-module writes go through the in-process event bus (`shared/events/bus.ts`)
- Cross-module reads use injected service interfaces, not direct imports
- Shared infra (DB pool, event bus, config) lives in `src/shared/` and is injected via Fastify plugin options
