import { useSyncExternalStore } from "react";

import type { AuthView } from "../auth/auth-session.types.js";
import { auth } from "../services.js";

export function useAuth(): AuthView {
  return useSyncExternalStore(auth.subscribe, auth.getView);
}
