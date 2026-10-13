import { setTimeout as delay } from "node:timers/promises";

// The description may be a function, so a message that quotes live state (such
// as a process's output) is built when the wait times out, not before it starts.
export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  description: string | (() => string),
  timeoutMs = 20_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    const now = Date.now();
    if (now > deadline) {
      const reason = typeof description === "string" ? description : description();
      throw new Error(`Timed out waiting for ${reason}`);
    }
    await delay(25);
  }
}
