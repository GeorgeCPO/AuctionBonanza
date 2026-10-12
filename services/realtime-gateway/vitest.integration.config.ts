import { defineConfig } from "vitest/config";

// These tests need the services from the root docker-compose.yml. A missing
// variable stops the run here instead of letting tests skip themselves, so a
// green run means every integration test ran.
const requiredVariables = ["TEST_KAFKA_BROKERS", "TEST_VALKEY_NODES", "TEST_VALKEY_TOPOLOGY_NODES"];

const missing = requiredVariables.filter((name) => process.env[name] === undefined);
if (missing.length > 0) {
  throw new Error(
    `Integration tests need ${missing.join(", ")}. Start the Compose stack and run ` +
      "`pnpm test:integration:local`, or set them yourself (see docs/TESTING.md).",
  );
}

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/integration/**/*.test.ts"],
  },
});
