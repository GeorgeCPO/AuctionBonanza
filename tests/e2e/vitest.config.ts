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
  },
});
