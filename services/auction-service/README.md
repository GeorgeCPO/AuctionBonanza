# Auction Service

Independently buildable and testable auction lifecycle service.

Its initial responsibility is auction lifecycle management and synchronous bid acceptance. Auctions and bids remain in one service initially because their correctness rules require a clear transactional boundary.

The service currently exposes:

- `POST /v1/auctions`, authenticated with an identity-service access token.
- `GET /v1/auctions`, a public newest-first listing with optional title search
  and cursor pagination.
- `GET /v1/auctions/:id`, a public resource read whose `phase` is derived using
  the PostgreSQL clock.
- `POST /v1/auctions/:id/bids`, authenticated with an identity-service access
  token. Accepts `{ amountMinor, idempotencyKey }` and returns the accepted
  bid alongside the auction's new `version`/`leadingBidId`/`updatedAt`: `201`
  for a fresh placement, `200` when a request replays an idempotency key that
  already succeeded.
- `GET /health` and `GET /ready` process and PostgreSQL health checks.

Money is represented as decimal minor-unit strings at the JSON boundary.
Creation idempotency is deferred: no idempotency key is required, and retrying a
request may create another auction. Clients should avoid automatic creation
retries when the outcome is unknown. Bid placement is the opposite: it
requires a client-supplied `idempotencyKey` scoped to (auction, bidder, key),
because a dropped response on a bid that actually qualified, if blindly
retried after someone else raised the price, would otherwise look like a
"too low" failure rather than the success it was. Reusing a key with a
different amount is rejected as a client error.

`GET /v1/auctions` accepts an optional `search` query parameter. It trims the
term and performs case-insensitive substring matching on titles; an empty term
lists all auctions. The literal characters `%` and `_` have no wildcard
meaning. No other filters are currently supported.

Listings are ordered by `created_at DESC, id DESC`. `limit` defaults to 20 and
must be an integer from 1 through 100. Pass the opaque `nextCursor` returned by
one response as `cursor` to read the next page, keeping the same `search` term.
Each response is `{ items, nextCursor }`: items use the same shape as the detail
endpoint, including phases derived using database time. IDs remain decimal
strings. The cursor retains PostgreSQL's microsecond timestamp precision even
though response timestamps use the existing millisecond ISO format; clients
must use the returned cursor rather than reconstructing it from an item.

The query fetches `limit + 1` rows and returns at most `limit` items. A `null` cursor means the
unchanged result set has been exhausted. Cursor pagination avoids duplicates
and omissions while traversing an unchanged dataset; like other keyset
pagination, concurrent inserts can change what a fresh traversal contains.
Pages are separate reads, not a shared snapshot: updates and deletions can also
change membership, and phases can advance between requests. Invalid limits or
malformed cursors return HTTP 400 with `code: "INVALID_REQUEST"`. Unknown query
parameters and repeated parameters are also rejected.

For example, request `/v1/auctions?search=clock&limit=20`, then request
`/v1/auctions?search=clock&limit=20&cursor=<nextCursor>` until `nextCursor` is null.

The initial creation rules, derived phases, terminal facts, bidding contract,
and time-sensitive transaction rules are recorded in
[`../../docs/AUCTION_LIFECYCLE.md`](../../docs/AUCTION_LIFECYCLE.md).

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
