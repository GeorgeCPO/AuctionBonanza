import { useEffect, useState, useSyncExternalStore } from "react";

import { LiveStream } from "./live-stream.js";
import type { LiveView } from "./live-stream.types.js";

// Keeps one SSE stream open for `ids` while the component is mounted. A key
// built from the ids keeps the effect from restarting on every render.
export function useLiveAuctions(ids: readonly string[]): LiveView {
  const [stream] = useState(() => new LiveStream());
  const view = useSyncExternalStore(stream.subscribe, stream.getView);
  const idsKey = ids.join(",");

  useEffect(() => {
    stream.watch(idsKey === "" ? [] : idsKey.split(","));
  }, [stream, idsKey]);

  useEffect(() => () => stream.close(), [stream]);

  return view;
}
