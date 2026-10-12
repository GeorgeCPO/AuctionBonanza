import { defineConfig } from "vitest/config";

// The suite skips itself when these are unset, which keeps local runs without
// infrastructure green. In CI a skip would hide untested behavior, so a
// missing variable fails the run before any test starts.
const integrationVariables = ["TEST_DATABASE_URL", "TEST_KAFKA_BROKERS", "TEST_VALKEY_NODES"];

if (process.env.CI === "true") {
  const missing = integrationVariables.filter((name) => process.env[name] === undefined);
  if (missing.length > 0) {
    throw new Error(`CI=true but end-to-end tests would be skipped; set ${missing.join(", ")}`);
  }
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
