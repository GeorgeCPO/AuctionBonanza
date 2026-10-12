import { setTimeout as delay } from "node:timers/promises";

export async function waitFor(
  condition: () => boolean,
  description: string,
  timeoutMs = 20_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    const now = Date.now();
    if (now > deadline) {
      throw new Error(`Timed out waiting for ${description}`);
    }
    await delay(25);
  }
}
