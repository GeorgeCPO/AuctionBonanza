// One-line description of an error for logs and dead-letter headers: name,
// driver or system code when there is one, then the message. The same helper
// as auction-service's, copied.
export function describeError(error: unknown): string {
  if (error instanceof Error) {
    const code =
      "code" in error && (typeof error.code === "string" || typeof error.code === "number")
        ? ` (${error.code})`
        : "";
    return `${error.name}${code}: ${error.message}`;
  }
  return String(error);
}
