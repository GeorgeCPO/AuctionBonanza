# Contracts

Narrow, versioned transport contracts shared by services.

`BidAccepted.v1` is the first event contract. Its exported TypeBox schema and
type guard/parser keep the static TypeScript shape tied to runtime validation.
IDs, aggregate versions, and minor-unit amounts remain decimal strings at the
JSON boundary so they never pass through JavaScript floating-point numbers.

This package must not become a general-purpose `common` package or contain
service-owned domain behavior.
