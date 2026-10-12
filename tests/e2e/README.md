# End-to-end tests

Follows behavior across services, with every service a black box. Each test
compiles the services it needs from source into a throwaway directory, runs
each entry point as its own process, and talks to them only over HTTP, the
database (to create fixtures such as users), and the infrastructure from the
root `docker-compose.yml`. Nothing here imports a service's code.

- `test/realtime-bid.test.ts`: a bid placed over HTTP reaches an SSE client,
  through the auction-service API, its outbox publisher, Kafka, the relay,
  Valkey, and the edge. identity-service is stood in for by a test issuer that
  signs the access tokens auction-service verifies.

Each run uses its own database (created from `db/schema.sql`), Kafka topic and
consumer group, and auction ids far from other tests' Valkey channels.

```sh
TEST_DATABASE_URL=postgresql://auction:auction_local@localhost:5432/auction_db \
TEST_KAFKA_BROKERS=localhost:9092 \
TEST_VALKEY_NODES=localhost:7001,localhost:7002,localhost:7003 \
pnpm --filter @auction-bonanza/e2e-tests test
```
