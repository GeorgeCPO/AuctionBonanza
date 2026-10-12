import { setTimeout as delay } from "node:timers/promises";

// Sleeps for `ms`, or until the signal aborts, whichever comes first. An abort
// is the normal way to stop a polling loop, so it is not an error.
export async function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  try {
    await delay(ms, undefined, { signal });
  } catch (error) {
    if (!signal.aborted) {
      throw error;
    }
  }
}
