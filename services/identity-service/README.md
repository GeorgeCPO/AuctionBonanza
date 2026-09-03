# Identity Service

Independently buildable and testable identity service.

Its initial responsibility is user registration and ownership of writes to user
data. Authentication, tokens, and sessions are intentionally out of scope for
this milestone. The service uses the shared `auction_db`, but other services
should not take over identity-specific write behavior.

## HTTP API

- `GET /health` reports process liveness without depending on PostgreSQL.
- `GET /ready` runs `SELECT 1` through the PostgreSQL pool and returns `503`
  while the database is unavailable.
- `POST /v1/users` accepts `email`, `password`, and optional `username`. Emails are
  stored lowercase, passwords are stored as salted Argon2id PHC strings, and
  duplicate emails return `409`.

## Runtime configuration

Configuration is parsed and validated before startup. Copy `.env.example` to
`.env` for local use.

| Variable       | Required | Default       | Meaning                                |
| -------------- | -------- | ------------- | -------------------------------------- |
| `NODE_ENV`     | No       | `development` | `development`, `test`, or `production` |
| `HOST`         | No       | `0.0.0.0`     | HTTP server bind host                  |
| `PORT`         | No       | `3001`        | Integer from 1 through 65535           |
| `DATABASE_URL` | Yes      | none          | PostgreSQL connection URL              |

```sh
pnpm build
pnpm test
pnpm start:local
```

The `users_email_case_insensitive_key` unique database index is the final
authority for registration uniqueness, including concurrent requests. The
insert uses SQL parameters; it does not rely on a race-prone pre-insert lookup.
