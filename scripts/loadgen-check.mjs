// The Go load tool's hermetic gate, the counterpart of `pnpm check` for
// tools/loadgen: formatting, vet (also over the integration-tagged test files,
// so they cannot rot unseen), and the unit tests with the race detector. It
// needs the Go toolchain but no infrastructure.
//
//   pnpm loadgen:check
//
// gofmt -l exits 0 even when files need formatting, so its output is checked.
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";

const toolDirectory = resolve(import.meta.dirname, "../tools/loadgen");

function run(label, command, args) {
  console.log(`> ${label}`);
  const result = spawnSync(command, args, { cwd: toolDirectory, encoding: "utf8" });
  if (result.error !== undefined) {
    console.error(`could not run ${command}: ${result.error.message} (is Go installed?)`);
    process.exit(1);
  }
  process.stdout.write(result.stdout);
  process.stderr.write(result.stderr);
  return result;
}

const unformatted = run("gofmt -l .", "gofmt", ["-l", "."]);
if (unformatted.status !== 0 || unformatted.stdout.trim() !== "") {
  console.error("These files need `gofmt -w`:\n" + unformatted.stdout);
  process.exit(1);
}

for (const [label, args] of [
  ["go vet ./...", ["vet", "./..."]],
  ["go vet -tags integration ./...", ["vet", "-tags", "integration", "./..."]],
  ["go test -race ./...", ["test", "-race", "./..."]],
]) {
  const result = run(label, "go", args);
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}
