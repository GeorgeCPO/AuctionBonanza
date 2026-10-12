import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    // Everything outside test/integration is a unit test. A file put in the
    // wrong folder then runs here, where missing infrastructure fails it,
    // instead of never running at all.
    exclude: [...configDefaults.exclude, "test/integration/**"],
  },
});
