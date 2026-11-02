import { useEffect, useRef, useState } from "react";

import { describeError } from "../error-messages.js";
import { useLiveAuctions } from "../live/use-live-auctions.js";
import { addMinor, formatMinor, formatMoney, parseMajorToMinor } from "../money.js";
import { auctionApi, auth } from "../services.js";
import type { BidResult, PendingBid } from "./auction-detail.types.js";
import { viewAuction } from "./auction-view.js";
import { StreamStatusBadge } from "./StreamStatusBadge.js";
import { useAuth } from "./use-auth.js";
import { useLoaded } from "./use-loaded.js";
import { useServerNow } from "./use-server-now.js";

export function AuctionDetail({ auctionId }: { auctionId: string }) {
  const { user } = useAuth();
  const {
    data: auction,
    error: loadError,
    reload,
  } = useLoaded(() => auctionApi.get(auctionId), [auctionId]);
  const [amount, setAmount] = useState("");
  const [bidResult, setBidResult] = useState<BidResult | undefined>();
  const [busy, setBusy] = useState(false);
  // The idempotency key of the bid being attempted. Resending the same amount
  // reuses it, so a retry after a lost response cannot place a second bid.
  const pendingBid = useRef<PendingBid | undefined>(undefined);

  const live = useLiveAuctions([auctionId]);
  const serverNowMs = useServerNow(live.clockOffsetMs);
  const view =
    auction === undefined
      ? undefined
      : viewAuction(auction, live.entries.get(auctionId), serverNowMs);
  const isEnded = view?.phase === "ended";

  // The stream sends no event when the finalizer closes an auction. Past its
  // end time, read the row again until its closedAt shows up.
  useEffect(() => {
    if (!isEnded) {
      return;
    }
    const timer = setInterval(reload, 3000);
    return () => clearInterval(timer);
  }, [isEnded, reload]);

  if (loadError !== undefined && auction === undefined) {
    return <p className="error">{loadError}</p>;
  }
  if (auction === undefined || view === undefined) {
    return <p>Loading…</p>;
  }

  const minimumNextBid = view.hasBid
    ? addMinor(view.priceMinor, auction.minimumBidIncrementMinor)
    : view.priceMinor;

  async function placeBid(): Promise<void> {
    const amountMinor = parseMajorToMinor(amount);
    if (amountMinor === undefined) {
      setBidResult({ ok: false, text: "Enter an amount like 12.50." });
      return;
    }
    if (pendingBid.current?.amountMinor !== amountMinor) {
      pendingBid.current = { amountMinor, idempotencyKey: crypto.randomUUID() };
    }
    setBusy(true);
    setBidResult(undefined);
    try {
      const accessToken = await auth.getAccessToken();
      if (accessToken === undefined) {
        setBidResult({ ok: false, text: "Sign in to bid." });
        return;
      }
      const placed = await auctionApi.placeBid(
        accessToken,
        auctionId,
        amountMinor,
        pendingBid.current.idempotencyKey,
      );
      pendingBid.current = undefined;
      setBidResult({
        ok: true,
        text: `Bid ${placed.bid.id} accepted at ${formatMinor(placed.bid.amountMinor)}.`,
      });
    } catch (caught) {
      // A rejected bid keeps its key: sending the same amount again is a retry.
      setBidResult({ ok: false, text: describeError(caught) });
    } finally {
      setBusy(false);
    }
  }

  return (
    <section>
      <h2>
        {auction.title} <StreamStatusBadge status={live.status} />
      </h2>
      <p>{auction.description}</p>
      <dl>
        <dt>Phase</dt>
        <dd>{view.phase}</dd>
        <dt>{view.hasBid ? "Leading bid" : "Starting price"}</dt>
        <dd className="price">{formatMoney(view.priceMinor, view.currencyCode)}</dd>
        <dt>Time left</dt>
        <dd>{view.countdown ?? "–"}</dd>
        <dt>Version</dt>
        <dd>{view.version ?? "–"}</dd>
        <dt>Increment</dt>
        <dd>{formatMoney(auction.minimumBidIncrementMinor, auction.currencyCode)}</dd>
        <dt>Owner</dt>
        <dd>{auction.ownerId}</dd>
      </dl>

      {user === null ? (
        <p>Sign in to place a bid.</p>
      ) : (
        <form
          onSubmit={(event) => {
            event.preventDefault();
            void placeBid();
          }}
        >
          <label>
            Your bid ({auction.currencyCode}){" "}
            <input
              inputMode="decimal"
              value={amount}
              placeholder={formatMinor(minimumNextBid)}
              onChange={(event) => setAmount(event.target.value)}
            />
          </label>{" "}
          <button type="submit" disabled={busy}>
            Place bid
          </button>
          {bidResult !== undefined && (
            <p className={bidResult.ok ? "ok" : "error"}>{bidResult.text}</p>
          )}
        </form>
      )}

      <h3>Stream log</h3>
      <ul className="log">
        {live.log.map((line) => (
          <li key={line.id}>
            <time>{line.receivedAt}</time> {line.text}
          </li>
        ))}
      </ul>
    </section>
  );
}
