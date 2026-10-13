# Contracts

Narrow, versioned transport contracts shared by services.

`BidAccepted.v1` is the first event contract. Its exported TypeBox schema and
type guard/parser keep the static TypeScript shape tied to runtime validation.
IDs, aggregate versions, and minor-unit amounts remain decimal strings at the
JSON boundary so they never pass through JavaScript floating-point numbers.

`findAuctionClosedV1Violation` names the first rule an `AuctionClosed.v1` candidate
breaks, and `parseAuctionClosedV1` includes it in its error. `EVENT_TYPE_HEADER` and
`EVENT_ID_HEADER` name the Kafka headers every event record carries, so the producer
and each consumer spell them from one place.

This package must not become a general-purpose `common` package or contain
service-owned domain behavior.
