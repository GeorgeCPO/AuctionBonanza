import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    // Everything outside test/integration is a unit test. A file put in the
    // wrong folder then runs here, where missing infrastructure fails it,
    // instead of never running at all.
    exclude: [...configDefaults.exclude, "test/integration/**"],
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.types.ts"],
      reporter: ["text", "html", "lcov"],
      reportsDirectory: "coverage/unit",
      // One point below the values measured when these were set; raise them
      // when coverage rises, so a drop shows up as a failure (docs/TESTING.md).
      thresholds: { statements: 60, branches: 71, functions: 63, lines: 59 },
    },
  },
});
