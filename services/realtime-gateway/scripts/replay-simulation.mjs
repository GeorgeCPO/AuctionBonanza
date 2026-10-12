// Replays one seed of the registry simulation, for example a seed a failed run
// printed:
//
//   pnpm test:sim:replay 123
import { spawnSync } from "node:child_process";

const [seed, ...extra] = process.argv.slice(2);
if (seed === undefined || !/^\d+$/.test(seed) || extra.length > 0) {
  console.error("usage: pnpm test:sim:replay <seed>   (a non-negative integer)");
  process.exit(2);
}

const result = spawnSync(
  "pnpm",
  [
    "exec",
    "vitest",
    "run",
    "-c",
    "vitest.unit.config.ts",
    "test/unit/auction-registry.simulation.test.ts",
  ],
  { stdio: "inherit", env: { ...process.env, SIM_SEED: seed } },
);
process.exit(result.status ?? 1);
