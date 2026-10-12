import { defineConfig } from "vitest/config";

// The suite needs the services from the root docker-compose.yml. A missing
// variable stops the run here instead of letting the suite skip itself, so a
// green run means the suite ran.
const requiredVariables = ["TEST_DATABASE_URL", "TEST_KAFKA_BROKERS", "TEST_VALKEY_NODES"];

const missing = requiredVariables.filter((name) => process.env[name] === undefined);
if (missing.length > 0) {
  throw new Error(
    `End-to-end tests need ${missing.join(", ")}. Start the Compose stack and run ` +
      "`pnpm test:e2e:local`, or set them yourself (see docs/TESTING.md).",
  );
}

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    // The suite's own timeout (180 s) covers its tests but not its hooks,
    // whose default is 10 s. beforeAll compiles two services and starts three
    // processes, which a loaded CI runner can take longer than 10 s to do.
    // The limit only turns a hang into a failure; the hook ends when it is
    // ready.
    hookTimeout: 180_000,
  },
});
