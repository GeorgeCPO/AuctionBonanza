// Log messages are part of the worker's operational contract: the tests match
// on them, and so do the people searching the logs. Defined once, as
// auction-service does for its outbox publisher and finalizer, so rewording one
// is a change in one place. The end-to-end suite cannot import service code and
// keeps its own copy of the one message it waits for.
export const SETTLEMENT_LOG_MESSAGES = {
  started: "settlement started",
  shutdownRequested: "shutdown requested; finishing in-flight messages",
  stopped: "settlement stopped",
  orderCreated: "order created",
  unsoldRecorded: "auction closed unsold; nothing to settle",
  duplicate: "event already settled; ignoring the repeat delivery",
  deadLettered: "record could not be settled; sent to the dead-letter topic",
  partitionPaused: "settlement failed; partition paused, offset not committed",
} as const;
