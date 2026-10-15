import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/unit/**/*.test.ts"],
    coverage: {
      provider: "v8",
      // Only the logic that has no UI: the React components are checked by
      // hand against the running stack (docs/FRONTEND.md).
      include: [
        "src/api/**/*.ts",
        "src/auth/**/*.ts",
        "src/live/live-auction-book.ts",
        "src/live/phase.ts",
        "src/money.ts",
        "src/error-messages.ts",
        "src/ui/route.ts",
      ],
      exclude: ["src/**/*.types.ts"],
      reporter: ["text", "html", "lcov"],
      reportsDirectory: "coverage/unit",
      thresholds: { statements: 80, branches: 75, functions: 80, lines: 80 },
    },
  },
});
