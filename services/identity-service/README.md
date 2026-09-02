# Identity Service

Independently buildable and testable foundation for the identity service.

Its initial responsibility is user registration, authentication, and ownership of writes to user data. The first implementation may use the shared `auction_db`, but other services should not take over identity-specific write behavior.

## Runtime configuration

Configuration is parsed and validated before startup. Copy `.env.example` to
`.env` for local use.

| Variable       | Required | Default       | Meaning                                |
| -------------- | -------- | ------------- | -------------------------------------- |
| `NODE_ENV`     | No       | `development` | `development`, `test`, or `production` |
| `HOST`         | No       | `0.0.0.0`     | Bind host for the future HTTP server   |
| `PORT`         | No       | `3001`        | Integer from 1 through 65535           |
| `DATABASE_URL` | Yes      | none          | PostgreSQL connection URL              |

```sh
pnpm build
pnpm test
pnpm start:local
```

The current entry point proves configuration loading and exits; the HTTP server
will be added in a later milestone.
