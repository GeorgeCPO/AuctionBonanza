// Adapted from the realtime gateway's test support: tests keep no code in
// common. Trimmed to what these tests use.

import { execFile, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { promisify } from "node:util";

import type { CompiledService, ServiceProcess } from "./service-process.types.js";

const serviceRoot = resolve(import.meta.dirname, "../..");

// Compiles the service into a throwaway directory under its node_modules, so
// dist/ is never overwritten and Node resolves its dependencies from it.
export async function compileService(prefix: string): Promise<CompiledService> {
  const cacheRoot = resolve(serviceRoot, "node_modules/.cache");
  await mkdir(cacheRoot, { recursive: true });
  const directory = await mkdtemp(resolve(cacheRoot, `${prefix}-`));
  await promisify(execFile)(
    resolve(serviceRoot, "node_modules/.bin/tsc"),
    ["-p", "tsconfig.build.json", "--outDir", directory],
    { cwd: serviceRoot },
  );
  return {
    root: serviceRoot,
    entry: (file) => resolve(directory, file),
    remove: () => rm(directory, { recursive: true, force: true }),
  };
}

export function startServiceProcess(
  service: CompiledService,
  entryFile: string,
  environment: Record<string, string>,
): ServiceProcess {
  const child = spawn(process.execPath, [service.entry(entryFile)], {
    cwd: service.root,
    // Only the explicit settings: nothing leaks in from a developer's shell.
    env: { PATH: process.env.PATH ?? "", NODE_ENV: "test", ...environment },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const exited = once(child, "exit") as Promise<[number | null, string | null]>;
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (chunk: Buffer) => (stdout += chunk.toString("utf8")));
  child.stderr?.on("data", (chunk: Buffer) => (stderr += chunk.toString("utf8")));

  return {
    process: child,
    stdout: () => stdout,
    stderr: () => stderr,
    logs: () =>
      stdout
        .split("\n")
        .filter((line) => line.startsWith("{"))
        .map((line) => JSON.parse(line) as Record<string, unknown>),
    exited,
    stop: async () => {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill("SIGTERM");
      }
      return exited;
    },
  };
}
