# HTTP Routing

Small Fastify adapter shared by HTTP services in this monorepo. It translates a
nested route tree into registered endpoints, including version segments and
colon-prefixed path parameters.

This package contains routing mechanics only. Request schemas, handlers, and
service dependencies remain owned by each service.
