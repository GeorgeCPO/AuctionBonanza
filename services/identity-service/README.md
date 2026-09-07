# Identity Service

Identity owns user registration, password authentication, access-token signing,
and refresh-session writes in the shared `auction_db`. The API is JSON and
client-neutral: mobile clients can call it directly and a future BFF can proxy
it without changing the identity contract.

HTTP handlers are limited to transport validation, response mapping, and
request-scoped logging. Application services own registration and authentication
behavior, while service-local PostgreSQL repositories own parameterized SQL,
row mapping, expected constraint translation, and refresh transaction mechanics.
The refresh repository exposes rotation as one atomic operation rather than as
independent CRUD calls that could escape the checked-out transaction connection.

## HTTP API

- `GET /health` is process liveness and does not query PostgreSQL.
- `GET /ready` queries PostgreSQL and returns `503` when it is unavailable.
- `POST /v1/users` registers a normalized email and Argon2id password hash.
- `POST /v1/sessions` accepts `{ "email", "password" }`. On success (`201`) it
  returns `accessToken`, `refreshToken`, `tokenType: "Bearer"`, `expiresIn`,
  `refreshExpiresAt`, and basic user data. Unknown emails and incorrect
  passwords have the same generic `401` response. The unknown-email path still
  performs the configured Argon2id work before failing.
- `POST /v1/sessions/refresh` accepts `{ "refreshToken" }`. On success (`201`)
  it atomically consumes that token and returns replacement access and refresh
  credentials. Invalid, expired, revoked, and replayed credentials return the
  same generic `401` response.
- `DELETE /v1/sessions/current` accepts `{ "refreshToken" }` and always returns
  `204` for a well-shaped request, whether the token is current, consumed,
  unknown, malformed, or already logged out. This prevents an existence oracle.
- `GET /v1/sessions/current` requires `Authorization: Bearer <access-token>`,
  verifies it using the shared strict verifier, and returns the matching user
  and session representation.
- `GET /.well-known/jwks.json` publishes public ES256 verification keys only.

Authentication failures use `401`, the body
`{ "code": "INVALID_AUTHENTICATION", "message": "Authentication failed" }`,
and a `WWW-Authenticate: Bearer` challenge. `403` is reserved for an already
authenticated principal that lacks permission.

## Credential responsibilities

Access tokens are ES256 JWTs valid for 10 minutes. They contain only `iss`,
`sub` (user ID string), `aud`, `iat`, `exp`, `jti`, and `sid` (session ID). The
protected header pins `alg: ES256`, identifies the key with `kid`, and uses
`typ: at+jwt`. Verifiers require every claim, pin issuer/audience/algorithm/type,
allow only the configured five-second clock skew, reject future issuance,
invalid string identifiers, unknown keys, and a lifetime over 600 seconds.
They never trust decoded-but-unverified content.

ES256 was chosen because `jose`, Node.js, and AWS KMS-compatible EC P-256 key
workflows support it, while its signatures are smaller than RSA signatures.
The algorithm is fixed in runtime configuration and again in every verifier;
token-controlled algorithm substitution is not allowed.

Refresh tokens are opaque 32-byte random secrets (256 bits), sent only to
identity. Only SHA-256 digests are stored. Passwords need deliberately slow,
salted Argon2id because human-chosen input has low entropy and is guessable.
A uniformly random 256-bit refresh secret is not feasibly enumerable, so a fast
SHA-256 digest is safe and permits indexed token lookup.

Mobile clients should keep the refresh token in OS-backed secure storage and
keep access tokens in the least persistent storage practical. A future BFF may
own browser-facing cookie policy separately. Browser applications must not put
either credential in `localStorage`. TLS is mandatory in production.

## Refresh transaction and replay behavior

Refresh checks out one PostgreSQL connection and explicitly executes `BEGIN`, a
digest lookup that locks both token and session with `FOR UPDATE`, validation
using database `now()`, consumption of the old row, insertion of a linked next
generation, session metadata update, and one `COMMIT`. The HTTP response is sent
only after commit. A database failure before commit rolls the transaction back
and the old token remains usable. Row locking ensures two concurrent uses cannot
both rotate successfully.

Consumed token rows are retained. Reuse of any consumed generation is treated
as possible credential replay: identity revokes the entire session family,
issues nothing, records a categorical security event, and returns the generic
authentication error. There is intentionally no grace window. If rotation
commits but its response is lost, a legitimate retry of the old token is
indistinguishable from replay; this initial policy revokes the family and
requires login again. An idempotency design would need durable, confidential
response recovery and a carefully bounded replay policy before adoption.

Session expiry is absolute at 30 days from login and is not extended by refresh.
Cleanup can delete expired or sufficiently old revoked `identity_sessions` in
batches by `expires_at`/`revoked_at`; `ON DELETE CASCADE` removes their token
history. Cleanup must retain consumed generations for the active family so
replay remains detectable.

Logout revokes refresh capability. Previously issued access tokens remain
locally verifiable until `exp`; therefore the maximum normal revocation delay is
the 10-minute access-token lifetime plus the verifier's five-second positive
clock-skew allowance. Password changes, account disabling, and “log out
everywhere” must revoke every refresh session for that user. Immediate global
access-token revocation would require an additional mechanism and is deferred.

## Signing keys and runtime configuration

Identity alone reads the PKCS#8 private key. The JWKS file contains one or more
public P-256 keys, allowing overlap during rotation. Generate ignored local
material from the repository root:

```sh
pnpm auth:keys:local local-2026-09
cp services/identity-service/.env.example services/identity-service/.env
pnpm --filter @auction-bonanza/identity-service build
pnpm --filter @auction-bonanza/identity-service start:local
```

Never commit a production private key. In production, inject a protected file
or replace this loader with an AWS KMS-backed signer while retaining the same
`kid`, algorithm, claims, and JWKS contract.

| Variable                          | Default                 | Meaning                                      |
| --------------------------------- | ----------------------- | -------------------------------------------- |
| `NODE_ENV`                        | `development`           | Runtime environment                          |
| `HOST` / `PORT`                   | `0.0.0.0` / `3001`      | HTTP bind address                            |
| `DATABASE_URL`                    | required                | PostgreSQL connection URL                    |
| `AUTH_ISSUER`                     | `http://localhost:3001` | Exact JWT issuer                             |
| `AUTH_AUDIENCE`                   | `auction-api`           | Logical backend audience                     |
| `ACCESS_TOKEN_TTL_SECONDS`        | `600`                   | Access-token lifetime                        |
| `REFRESH_SESSION_TTL_SECONDS`     | `2592000`               | Absolute refresh-session lifetime            |
| `ACCESS_TOKEN_CLOCK_SKEW_SECONDS` | `5`                     | Verification tolerance                       |
| `AUTH_SIGNING_ALGORITHM`          | `ES256`                 | Pinned JOSE algorithm                        |
| `AUTH_ACTIVE_KEY_ID`              | required                | `kid` used for new signatures                |
| `AUTH_SIGNING_PRIVATE_KEY_PATH`   | required                | Active PKCS#8 private-key path               |
| `AUTH_JWKS_PATH`                  | required                | Public JWKS path, including the active `kid` |

Rotate keys by publishing the new public JWK first, deploying the new private
key and active `kid` second, retaining the old public key for at least the
maximum access-token lifetime plus clock skew and cache propagation, then
removing it. Identity validates at startup that the active public key matches
the private key.

Downstream services cache at most eight public keys for a bounded TTL. A known,
fresh cached key requires no identity or PostgreSQL request. An unknown `kid`
causes one JWKS refresh. A cold service with no cached JWKS fails authentication
closed if identity is unavailable; readiness/deployment ordering or a securely
provisioned bootstrap JWKS can address that availability tradeoff later.

## Operations and deferred work

Login and refresh require distributed/API-edge rate limiting before internet
exposure. An in-memory per-instance limiter is not sufficient. Emit metrics for
login successes/failures, refresh successes/failures, replay detection, JWKS
refresh failures, unknown key IDs, and signing failures. Logs may contain
request, user, session, and key IDs plus categorical outcomes; logger redaction
covers passwords, bearer headers, tokens, digests, and private keys.

Device/session management UI, federated login, service credentials, mTLS/token
exchange, immediate access-token revocation, and BFF/browser cookie/CSRF policy
are deferred. A user access token is initiating-user context, not proof of the
calling service's identity.
