import { useEffect, useState } from "react";

// The current time on the server's clock, re-read every second so countdowns
// tick. `clockOffsetMs` is server time minus local time (LiveView).
export function useServerNow(clockOffsetMs: number): number {
  const [localNow, setLocalNow] = useState(() => Date.now());

  useEffect(() => {
    const timer = setInterval(() => setLocalNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  return localNow + clockOffsetMs;
}
