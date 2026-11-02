# Roadmap

Failure and scale scenarios the system is meant to handle, each followed by the expected outcome. Current behavior is documented under [`docs/`](docs/); the entries here are targets, not a status report.

Kill an auction-engine instance halfway through handling a message.

No incorrect winner.

Deliver the same Kafka event five times.

No duplicate charge/order.

Lose a DB connection after committing but before acknowledging Kafka.

System remains correct.

Take one Kafka broker down.

Understand exactly what happens.

Make one Kafka partition extremely hot.

Diagnose and redesign it.

Let a consumer fall 10 million messages behind.

Recover it safely.

Rebuild the entire read model by replaying Kafka.

Run a DB migration while receiving 5k bids/sec.

No outage.

Query 500 million bid rows.

Get an initially awful query down to an acceptable execution time through partitioning/indexing/query redesign.

Split one PostgreSQL database into four shards.

Application continues running.

At the end i should be able to explain what happened in every one of those scenarios, in order to reduce Kafka and database blind spots.
