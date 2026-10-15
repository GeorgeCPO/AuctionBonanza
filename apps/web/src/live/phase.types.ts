export interface PhaseInput {
  startsAt: string;
  endsAt: string;
  cancelledAt: string | null;
  closedAt: string | null;
}

// `ended` is past endsAt but not yet closed: the finalizer has not picked a
// winner yet.
export type DisplayPhase = "scheduled" | "open" | "ended" | "closed" | "cancelled";
