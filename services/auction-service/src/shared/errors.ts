// The width of the error columns (`last_publish_error`,
// `last_finalization_error`).
export const MAX_STORED_ERROR_LENGTH = 1_000;

// One-line description of an error for logs and stored failure messages:
// name, driver or system code when there is one, then the message.
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

// An error as stored in an error column: described, without NUL characters
// (PostgreSQL text cannot hold them, and a rejected write would stop the
// failure from being recorded), and cut to the column width.
export function describeStoredError(error: unknown): string {
  return describeError(error).replaceAll("\u0000", "").slice(0, MAX_STORED_ERROR_LENGTH);
}
