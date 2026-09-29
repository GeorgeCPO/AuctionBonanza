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
- `GET /v1/auctions/live-states?ids=1,2,3`, a public bulk read of what a live
  view displays: version, leading bid, schedule, terminal facts, and derived
  phase, for 1 to 100 auction IDs. Items follow request order, repeated IDs are
  returned once, and unknown IDs are listed in `missingIds`. Every state comes
  from one statement, so a version is never paired with another version's
  leading bid. The realtime gateway uses it for snapshots and resyncs.
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

## Outbox publisher worker

`src/outbox-publisher.main.ts` is a second, independently deployable entry
point in this service. It shares the service's code and its ownership of
`outbox_events`, but it runs as its own process with its own validated
configuration, and it scales separately from the HTTP API. It claims committed
outbox events, publishes each stored payload to Kafka keyed by `auctionId`, and
marks an event published only after Kafka acknowledges it. The design, delivery
and ordering guarantees, and failure behavior are in
[`../../docs/OUTBOX_PUBLISHING.md`](../../docs/OUTBOX_PUBLISHING.md).

```sh
cp services/auction-service/.env.outbox-publisher.example services/auction-service/.env.outbox-publisher
pnpm --filter @auction-bonanza/auction-service build
pnpm --filter @auction-bonanza/auction-service start:outbox-publisher:local
```

The worker validates all configuration before connecting and exits with code 1
if any setting is invalid. It also rejects combinations that could make
PostgreSQL end a claim while Kafka might still acknowledge it.

| Variable                                   | Default                    | Meaning                                                             |
| ------------------------------------------ | -------------------------- | ------------------------------------------------------------------- |
| `DATABASE_URL`                             | required                   | PostgreSQL connection string                                        |
| `KAFKA_BROKERS`                            | `localhost:9092`           | Comma-separated bootstrap brokers                                   |
| `KAFKA_CLIENT_ID`                          | `auction-outbox-publisher` | Kafka client ID, also used as the PostgreSQL `application_name`     |
| `AUCTION_EVENTS_TOPIC`                     | `auction.events`           | Destination topic                                                   |
| `OUTBOX_PUBLISHER_CONCURRENCY`             | `1`                        | Publication loops, and PostgreSQL connections (1–16)                |
| `OUTBOX_BATCH_SIZE`                        | `50`                       | Maximum events per cycle, at most one per auction (1–500)           |
| `OUTBOX_POLL_INTERVAL_MS`                  | `250`                      | Pause after a cycle that was not a full, clean batch                |
| `OUTBOX_PUBLISH_TIMEOUT_MS`                | `10000`                    | Kafka delivery deadline per event                                   |
| `OUTBOX_RETRY_BASE_DELAY_MS`               | `500`                      | First retry backoff ceiling                                         |
| `OUTBOX_RETRY_MAX_DELAY_MS`                | `60000`                    | Maximum retry backoff ceiling                                       |
| `OUTBOX_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS` | `30000`                    | Releases a stalled publisher's locks; ≥ publish timeout + 5000      |
| `OUTBOX_SHUTDOWN_TIMEOUT_MS`               | `30000`                    | Graceful shutdown deadline; ≥ publish timeout + 7000                |
| `LOG_LEVEL`                                | `info`                     | Pino level; `debug` logs each published event with partition/offset |

On `SIGTERM` or `SIGINT`, the worker stops starting new cycles, lets in-flight
cycles finish and commit, flushes and disconnects the Kafka producer, and
closes its pool. A second signal, or reaching the shutdown deadline, exits
immediately. Uncommitted claims then roll back and are republished later.
