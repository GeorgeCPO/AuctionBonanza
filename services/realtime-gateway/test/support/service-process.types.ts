import type { ChildProcess } from "node:child_process";

export interface CompiledService {
  root: string;
  // Absolute path of a compiled entry point, e.g. entry("edge.main.js").
  entry(file: string): string;
  remove(): Promise<void>;
}

export interface ServiceProcess {
  process: ChildProcess;
  stdout(): string;
  stderr(): string;
  // Parsed JSON log lines from stdout.
  logs(): Record<string, unknown>[];
  waitForLog(message: string, timeoutMs?: number): Promise<Record<string, unknown>>;
  // Resolves with the exit code and signal once the process exits.
  exited: Promise<[number | null, string | null]>;
  // Sends SIGTERM and resolves with the exit code and signal.
  stop(): Promise<[number | null, string | null]>;
}
