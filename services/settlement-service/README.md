# settlement-service

The worker that turns a closed auction into an order. It consumes
`AuctionClosed.v1` from `auction.events` and creates one order for the winner of
each sold auction, idempotently. It has no HTTP API.

The design, the failure behavior (retries, the dead-letter topic, what each
crash does) and the configuration are in
[`docs/SETTLEMENT.md`](../../docs/SETTLEMENT.md).

## Layout

| File                           | Role                                                                            |
| ------------------------------ | ------------------------------------------------------------------------------- |
| `src/settlement.main.ts`       | The process: wiring and graceful shutdown.                                      |
| `src/settlement.consumer.ts`   | The Kafka consumer: acknowledgement boundary, pausing a failing partition.      |
| `src/settlement.handler.ts`    | One record: decide, settle or dead-letter. Knows nothing about Kafka.           |
| `src/settlement.decision.ts`   | Pure: ignore, settle or reject a record.                                        |
| `src/settlement.repository.ts` | The transaction: record the event, insert the order.                            |
| `src/settlement.errors.ts`     | Splits permanent database errors (dead-lettered) from transient ones (retried). |
| `src/dead-letter.record.ts`    | Pure: builds the dead-letter record.                                            |
| `src/dead-letter.publisher.ts` | The Kafka producer for `auction.events.dlq`.                                    |
| `src/settlement.config.ts`     | Validated configuration.                                                        |

## Running

```sh
docker compose up -d postgres kafka
docker compose up kafka-init
cp .env.settlement.example .env.settlement
pnpm --filter @auction-bonanza/settlement-service build
pnpm --filter @auction-bonanza/settlement-service start:local
```

## Testing

```sh
pnpm --filter @auction-bonanza/settlement-service test:unit
pnpm --filter @auction-bonanza/settlement-service test:integration:local
```

Unit tests need no infrastructure. Integration tests run the real consumer, handler
and repository against PostgreSQL and Kafka from the root `docker-compose.yml`, in
a throwaway database and per-test topics; see [`docs/TESTING.md`](../../docs/TESTING.md).
