// Bounds how long a caller waits for an operation. On timeout the caller gets
// an error and the operation is left running: Kafka's client scans for expired
// sends periodically, not exactly, so its own timeout cannot be relied on to
// fire on time. The operation may therefore still succeed after the caller
// gave up, which is why callers retry only idempotent work.
export async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
  description: string,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${description} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  try {
    return await Promise.race([operation, timeout]);
  } finally {
    clearTimeout(timer);
    // If the timeout won, a later failure of the operation has no one left to
    // handle it, and would crash the process as an unhandled rejection.
    operation.catch(() => {});
  }
}
