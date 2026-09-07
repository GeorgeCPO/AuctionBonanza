# Auction Service

Independently buildable and testable auction lifecycle service.

Its initial responsibility is auction lifecycle management and synchronous bid acceptance. Auctions and bids remain in one service initially because their correctness rules require a clear transactional boundary.

The service currently exposes:

- `POST /v1/auctions`, authenticated with an identity-service access token.
- `GET /v1/auctions/:id`, a public resource read whose `phase` is derived using
  the PostgreSQL clock.
- `GET /health` and `GET /ready` process and PostgreSQL health checks.

Money is represented as decimal minor-unit strings at the JSON boundary.
Creation idempotency is deferred: no idempotency key is required, and retrying a
request may create another auction. Clients should avoid automatic creation
retries when the outcome is unknown.

The initial creation rules, derived phases, terminal facts, and time-sensitive
transaction rules are recorded in [`../../docs/AUCTION_LIFECYCLE.md`](../../docs/AUCTION_LIFECYCLE.md).

`src/authentication.ts` wires the narrow shared access-token verifier to a
bounded remote-JWKS cache. The create route's Fastify authentication hook parses
the bearer header and attaches the returned `{ userId, sessionId, tokenId }`
principal. Known keys verify locally without an
identity-service or PostgreSQL call. On cold start the first verification must
fetch JWKS and fails closed when identity is unavailable; an unknown `kid`
forces a refresh so overlapping signing-key rotation works.

Authorization remains auction-owned behavior. The initiating user's access
token is not the auction service's identity for internal calls; service
credentials or mTLS/token exchange are a separate future design.

For local startup:

```sh
cp services/auction-service/.env.example services/auction-service/.env
pnpm --filter @auction-bonanza/auction-service build
pnpm --filter @auction-bonanza/auction-service start:local
```
