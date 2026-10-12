// Runs a command with the development-only test endpoints from
// scripts/local-test.env in its environment:
//
//   node scripts/with-local-test-env.mjs pnpm run test:integration
//
// The pnpm test:*:local scripts use it. `node --run` cannot do this job: it
// ignores --env-file. Variables already set in the shell win over the file.
import { spawn } from "node:child_process";
import { resolve } from "node:path";

const [command, ...args] = process.argv.slice(2);
if (command === undefined) {
  console.error("usage: node scripts/with-local-test-env.mjs <command> [args...]");
  process.exit(2);
}

process.loadEnvFile(resolve(import.meta.dirname, "local-test.env"));

const child = spawn(command, args, { stdio: "inherit" });
child.on("error", (error) => {
  console.error(`could not start ${command}: ${error.message}`);
  process.exit(1);
});
child.on("exit", (code, signal) => {
  if (signal !== null) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 1);
});
