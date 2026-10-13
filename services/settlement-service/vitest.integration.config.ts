import { defineConfig } from "vitest/config";

// These tests need the services from the root docker-compose.yml. A missing
// variable stops the run here instead of letting tests skip themselves, so a
// green run means every integration test ran.
const requiredVariables = ["TEST_DATABASE_URL", "TEST_KAFKA_BROKERS"];

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
    // Informational: nothing fails on a low number (see docs/TESTING.md). Only
    // code that runs inside the Vitest process is measured, not the service
    // the *-process tests start.
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.types.ts"],
      reporter: ["text", "html", "lcov"],
      reportsDirectory: "coverage/integration",
    },
  },
});
