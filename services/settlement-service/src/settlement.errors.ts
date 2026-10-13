// A failure that retrying cannot fix: the database refused this event's data,
// or its recorded state contradicts the event, and will do so again. It is
// reported apart from every other error because the two get opposite treatment. A transient error (the database is down, a
// deadlock, a timeout) is retried until it clears. A permanent one goes to the
// dead-letter topic, so one bad record cannot stall every auction on its
// partition.
export class PermanentSettlementError extends Error {
  // The SQLSTATE PostgreSQL raised. Undefined when settlement itself found the
  // event contradicted by the data, so no database error exists to quote.
  readonly sqlState: string | undefined;
  readonly constraint: string | undefined;

  constructor(
    message: string,
    details: { sqlState: string | undefined; constraint: string | undefined },
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "PermanentSettlementError";
    this.sqlState = details.sqlState;
    this.constraint = details.constraint;
  }
}

// SQLSTATE classes whose errors depend on the data, not on timing:
//   22 data exception (for example a bigint out of range)
//   23 integrity constraint violation (unique, foreign key, check, and the
//      check_violation raised by our triggers)
// Anything else is treated as transient and retried: connection loss, class 08;
// serialization and deadlock, class 40; lock and statement timeouts, classes 55
// and 57; and any error this list does not know. Retrying an error that is
// really permanent stalls one partition loudly, in the logs. Dead-lettering an
// error that is really transient would settle nothing until a person noticed,
// so the default errs toward retrying.
const PERMANENT_SQLSTATE_CLASSES = ["22", "23"];

// Turns a permanent database error into a PermanentSettlementError and returns
// every other error unchanged.
export function classifySettlementError(error: unknown): unknown {
  if (!(error instanceof Error)) {
    return error;
  }
  const { code, constraint } = error as Error & { code?: unknown; constraint?: unknown };
  if (typeof code !== "string" || code.length !== 5) {
    return error;
  }
  const permanent = PERMANENT_SQLSTATE_CLASSES.some((sqlStateClass) =>
    code.startsWith(sqlStateClass),
  );
  if (!permanent) {
    return error;
  }
  return new PermanentSettlementError(
    error.message,
    { sqlState: code, constraint: typeof constraint === "string" ? constraint : undefined },
    { cause: error },
  );
}
