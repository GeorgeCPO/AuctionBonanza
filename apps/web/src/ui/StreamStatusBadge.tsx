import type { StreamStatus } from "../live/live-stream.types.js";

export function StreamStatusBadge({ status }: { status: StreamStatus }) {
  return <span className={`status status-${status}`}>stream: {status}</span>;
}
