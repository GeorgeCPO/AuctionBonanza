import { ApiError } from "./api/http.js";
import { formatMinor } from "./money.js";

// Turns a failed call into one line for the user. The codes are the ones the
// services return (see the API sections of the service READMEs).
export function describeError(error: unknown): string {
  if (!(error instanceof ApiError)) {
    return error instanceof Error ? error.message : "Something went wrong";
  }
  const { body } = error;
  switch (error.code) {
    case "BID_TOO_LOW": {
      const minimum = body.minimumQualifyingAmountMinor;
      return typeof minimum === "string"
        ? `Bid too low: the minimum is ${formatMinor(minimum)}.`
        : "Bid too low.";
    }
    case "AUCTION_NOT_OPEN":
      return `The auction is not open (${typeof body.phase === "string" ? body.phase : "unknown phase"}).`;
    case "SELF_BID_NOT_ALLOWED":
      return "You cannot bid on your own auction.";
    case "IDEMPOTENCY_KEY_REUSED":
      return "That bid was already sent with a different amount.";
    case "UNAUTHORIZED":
    case "INVALID_AUTHENTICATION":
      return "Not signed in, or the session expired. Sign in again.";
    case "EMAIL_ALREADY_REGISTERED":
      return "That email is already registered. Use Sign in.";
    default:
      return `${error.code}: ${error.message}`;
  }
}
