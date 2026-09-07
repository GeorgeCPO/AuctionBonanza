# Auction Service

Independently buildable and testable foundation for the auction service.

Its initial responsibility is auction lifecycle management and synchronous bid acceptance. Auctions and bids remain in one service initially because their correctness rules require a clear transactional boundary.

The current entry point is intentionally minimal. Domain behavior and runtime
configuration will be added in later milestones.

The initial creation rules, derived phases, terminal facts, and time-sensitive
transaction rules are recorded in [`../../docs/AUCTION_LIFECYCLE.md`](../../docs/AUCTION_LIFECYCLE.md).

`src/authentication.ts` wires the narrow shared access-token verifier to a
bounded remote-JWKS cache. Once real HTTP business routes are introduced, their
Fastify authentication hook can parse the bearer header and attach the returned
`{ userId, sessionId, tokenId }` principal. Known keys verify locally without an
identity-service or PostgreSQL call. On cold start the first verification must
fetch JWKS and fails closed when identity is unavailable; an unknown `kid`
forces a refresh so overlapping signing-key rotation works.

Authorization remains auction-owned behavior. The initiating user's access
token is not the auction service's identity for internal calls; service
credentials or mTLS/token exchange are a separate future design.
