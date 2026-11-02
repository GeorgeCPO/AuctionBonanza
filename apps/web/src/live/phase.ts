import type { DisplayPhase, PhaseInput } from "./phase.types.js";

// The phase the user should see at `nowMs`. A snapshot's own `phase` is only
// right as of the moment it was read, so time-based transitions (scheduled to
// open, open to ended) come from the dates, with `nowMs` on the server's clock.
export function derivePhase(input: PhaseInput, nowMs: number): DisplayPhase {
  if (input.cancelledAt !== null) {
    return "cancelled";
  }
  if (input.closedAt !== null) {
    return "closed";
  }
  const startsAtMs = Date.parse(input.startsAt);
  const endsAtMs = Date.parse(input.endsAt);
  if (nowMs < startsAtMs) {
    return "scheduled";
  }
  if (nowMs < endsAtMs) {
    return "open";
  }
  return "ended";
}

// "1h 02m 03s" until `targetIso`, or undefined once it has passed.
export function formatCountdown(targetIso: string, nowMs: number): string | undefined {
  const remainingSeconds = Math.floor((Date.parse(targetIso) - nowMs) / 1000);
  if (remainingSeconds <= 0) {
    return undefined;
  }
  const hours = Math.floor(remainingSeconds / 3600);
  const minutes = Math.floor((remainingSeconds % 3600) / 60);
  const seconds = remainingSeconds % 60;
  const mm = String(minutes).padStart(2, "0");
  const ss = String(seconds).padStart(2, "0");
  if (hours > 0) {
    return `${hours}h ${mm}m ${ss}s`;
  }
  return `${minutes}m ${ss}s`;
}
