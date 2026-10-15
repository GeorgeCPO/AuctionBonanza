import { useState } from "react";

import { describeError } from "../error-messages.js";
import { parseMajorToMinor } from "../money.js";
import { auctionApi, auth } from "../services.js";
import { useAuth } from "./use-auth.js";

// The service rejects auctions shorter than a minute, measured when the request
// arrives, so the shortest preset keeps a margin above that.
const DURATIONS: readonly { label: string; ms: number }[] = [
  { label: "2 minutes", ms: 2 * 60_000 },
  { label: "5 minutes", ms: 5 * 60_000 },
  { label: "1 hour", ms: 60 * 60_000 },
  { label: "1 day", ms: 24 * 60 * 60_000 },
];

export function CreateAuctionForm() {
  const { user } = useAuth();
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [currencyCode, setCurrencyCode] = useState("USD");
  const [durationMs, setDurationMs] = useState(DURATIONS[0]?.ms ?? 2 * 60_000);
  const [startingPrice, setStartingPrice] = useState("10.00");
  const [increment, setIncrement] = useState("1.00");
  const [error, setError] = useState<string | undefined>();
  const [busy, setBusy] = useState(false);

  async function submit(): Promise<void> {
    const startingPriceMinor = parseMajorToMinor(startingPrice);
    const minimumBidIncrementMinor = parseMajorToMinor(increment);
    if (startingPriceMinor === undefined || minimumBidIncrementMinor === undefined) {
      setError("Prices must be amounts like 10.00.");
      return;
    }
    setBusy(true);
    setError(undefined);
    try {
      const accessToken = await auth.getAccessToken();
      if (accessToken === undefined) {
        setError("Sign in to create an auction.");
        return;
      }
      const created = await auctionApi.create(accessToken, {
        title,
        description,
        currencyCode,
        endsAt: new Date(Date.now() + durationMs).toISOString(),
        startingPriceMinor,
        minimumBidIncrementMinor,
      });
      window.location.hash = `#/auctions/${created.id}`;
    } catch (caught) {
      setError(describeError(caught));
    } finally {
      setBusy(false);
    }
  }

  if (user === null) {
    return <p>Sign in to create an auction.</p>;
  }

  return (
    <section>
      <h2>New auction</h2>
      <form
        className="stack"
        onSubmit={(event) => {
          event.preventDefault();
          void submit();
        }}
      >
        <label>
          Title
          <input
            value={title}
            maxLength={160}
            onChange={(event) => setTitle(event.target.value)}
            required
          />
        </label>
        <label>
          Description
          <textarea
            value={description}
            onChange={(event) => setDescription(event.target.value)}
            required
          />
        </label>
        <label>
          Currency
          <select value={currencyCode} onChange={(event) => setCurrencyCode(event.target.value)}>
            <option>USD</option>
            <option>EUR</option>
            <option>GBP</option>
          </select>
        </label>
        <label>
          Runs for
          <select
            value={durationMs}
            onChange={(event) => setDurationMs(Number(event.target.value))}
          >
            {DURATIONS.map((duration) => (
              <option key={duration.ms} value={duration.ms}>
                {duration.label}
              </option>
            ))}
          </select>
        </label>
        <label>
          Starting price
          <input value={startingPrice} onChange={(event) => setStartingPrice(event.target.value)} />
        </label>
        <label>
          Minimum increment
          <input value={increment} onChange={(event) => setIncrement(event.target.value)} />
        </label>
        <button type="submit" disabled={busy}>
          Create
        </button>
        {error !== undefined && <p className="error">{error}</p>}
      </form>
    </section>
  );
}
