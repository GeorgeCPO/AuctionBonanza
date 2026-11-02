import { useCallback, useEffect, useState } from "react";
import type { DependencyList } from "react";

import { describeError } from "../error-messages.js";
import type { LoadedState } from "./use-loaded.types.js";

// Runs `load` on mount, whenever `deps` change, and on `reload()`. A result that
// arrives after the next load started or the component unmounted is dropped.
// `load` itself is left out of the effect's dependencies on purpose: callers
// pass a fresh arrow every render, and `deps` says what it depends on.
export function useLoaded<T>(load: () => Promise<T>, deps: DependencyList): LoadedState<T> {
  const [data, setData] = useState<T | undefined>();
  const [error, setError] = useState<string | undefined>();
  const [reloads, setReloads] = useState(0);

  useEffect(() => {
    let cancelled = false;
    load()
      .then((loaded) => {
        if (!cancelled) {
          setData(loaded);
          setError(undefined);
        }
      })
      .catch((caught: unknown) => {
        if (!cancelled) {
          setError(describeError(caught));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [...deps, reloads]);

  const reload = useCallback(() => setReloads((count) => count + 1), []);
  return { data, error, reload };
}
