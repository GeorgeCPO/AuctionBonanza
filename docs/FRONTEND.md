# Web UI

`apps/web` is a small React + TypeScript single-page app (Vite) that makes the
backend visible: prices moving live, bid rejections, reconnects and resyncs. It
is deliberately minimal (no UI library, router, or state library) because the
project is about the backend. It is not deployed and is not in
`docker-compose.yml`; run it with `pnpm web:dev`.

## How it reaches the backend

The services set no CORS headers (the edge says so in
[`REALTIME_GATEWAY.md`](REALTIME_GATEWAY.md)). Instead of adding CORS to three
servers, the Vite dev server proxies one prefix per service, so the browser
sees a single origin:

| Prefix       | Service                 | Port |
| ------------ | ----------------------- | ---- |
| `/identity/` | identity-service        | 3001 |
| `/auction/`  | auction-service         | 3002 |
| `/gateway/`  | realtime-gateway (edge) | 3003 |

The prefix is stripped before forwarding (`/gateway/v1/stream` becomes
`/v1/stream`). A production setup would put the same routing in a reverse
proxy; that is a later decision.

## Decisions

- **Tokens live in memory only.** The identity service forbids `localStorage`
  for tokens, and a cookie-based BFF is deferred, so a page reload signs the
  user out. `AuthSession` (`src/auth/auth-session.ts`) owns the tokens and
  shares one refresh between concurrent callers: refresh tokens are single use,
  and sending one twice would look like theft and revoke the session. Any
  failed refresh, including a lost response, ends the session, because the
  client cannot tell whether the token was consumed.
- **Price, leading bid and version come from the SSE stream.** `LiveStream`
  (`src/live/live-stream.ts`) opens one `EventSource` for the visible auctions.
  The set is fixed per connection, so changing it reconnects. Rules from
  [`REALTIME_GATEWAY.md`](REALTIME_GATEWAY.md#client-protocol), in
  `LiveAuctionBook`: a snapshot replaces an auction's state; a `bid-accepted`
  event applies only if its version is greater (compared as `BigInt`). On a
  dropped stream the client reconnects with backoff, and the fresh snapshots
  replace what was shown, so it needs no gap handling.
- **Closing is not on the stream.** The gateway sends no event when the
  finalizer closes an auction (`phase` is only as of the last read), so a
  snapshot can predate `closedAt`. `viewAuction` (`src/ui/auction-view.ts`)
  therefore also reads `closedAt` and `cancelledAt` from the HTTP row, and the
  detail page re-reads that row every 3 seconds while the phase is `ended`.
- **Time comes from the server.** Each snapshot carries `serverTime`; the
  countdown uses the offset from the local clock. The displayed phase is derived
  from `startsAt`, `endsAt`, `closedAt` and `cancelledAt` (`src/live/phase.ts`),
  because a snapshot's `phase` is only right as of when it was read. After
  `endsAt` and before the finalizer runs the phase shows `ended`.
- **Money is a string of minor units end to end** (`src/money.ts`); the UI
  never converts it to a number.
- **A bid keeps its idempotency key across retries** of the same amount, so a
  retry after a lost response cannot place a second bid.

## Known gaps

- The auction list has no leading bid, so rows show the starting price until the
  stream delivers a snapshot.
- There is no "my bids", "my auctions" or orders view: the backend has no
  endpoints for them (settlement has no HTTP API).
- Streams are unauthenticated, matching the edge today.
- Only the first page of auctions (20) is shown.
- The list does not refresh `closed` or `cancelled` until "Reload list"; only
  the detail page polls for it.

## Testing

Logic without UI (`LiveAuctionBook`, `AuthSession`, the HTTP client, money,
phase, routing, error messages) has unit tests in `apps/web/test/unit`, run by
`pnpm test:unit` and gated by coverage thresholds like the services. The React
components and the live connection have no automated tests; check them by hand:

1. Start the stack as in the README and run `pnpm web:dev`.
2. In two browser windows, register two users; create a 2-minute auction in one.
3. Bid from the other: the price, leading bid and version update live in both.
4. Bid too low, and bid on your own auction: the error text appears.
5. Wait for the end: the phase goes `open`, `ended`, then `closed` once the
   finalizer runs.
6. Stop and restart the edge: the status goes to `reconnecting`, then `open`, and
   the log shows new snapshots.
