import { useEffect, useRef, useState } from "react";

import type { AuctionJson } from "../api/api.types.js";
import { describeError } from "../error-messages.js";
import { formatCountdown } from "../live/phase.js";
import { useLiveAuctions } from "../live/use-live-auctions.js";
import { addMinor, formatMinor, formatMoney, parseMajorToMinor } from "../money.js";
import { auctionApi, auth } from "../services.js";
import type { PendingBid } from "./auction-detail.types.js";
import { viewAuction } from "./auction-view.js";
import { useAuth } from "./use-auth.js";
import { useServerNow } from "./use-server-now.js";

export function AuctionDetail({ auctionId }: { auctionId: string }) {
  const { user } = useAuth();
  const [auction, setAuction] = useState<AuctionJson | undefined>();
  const [loadError, setLoadError] = useState<string | undefined>();
  const [amount, setAmount] = useState("");
  const [bidMessage, setBidMessage] = useState<string | undefined>();
  const [bidFailed, setBidFailed] = useState(false);
  const [busy, setBusy] = useState(false);
  // The idempotency key of the bid being attempted. Resending the same amount
  // reuses it, so a retry after a lost response cannot place a second bid.
  const pendingBid = useRef<PendingBid | undefined>(undefined);

  useEffect(() => {
    let cancelled = false;
    auctionApi
      .get(auctionId)
      .then((loaded) => {
        if (!cancelled) {
          setAuction(loaded);
        }
      })
      .catch((caught: unknown) => {
        if (!cancelled) {
          setLoadError(describeError(caught));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [auctionId]);

  const live = useLiveAuctions([auctionId]);
  const serverNowMs = useServerNow(live.clockOffsetMs);
  const view =
    auction === undefined
      ? undefined
      : viewAuction(auction, live.entries.get(auctionId), serverNowMs);
  const isEnded = view?.phase === "ended";

  // The fetched row never refreshes by itself, and the stream may have no state
  // for the auction (no bids yet, or the edge is down). Past its end time, read
  // it again until the finalizer's closedAt shows up.
  useEffect(() => {
    if (!isEnded) {
      return;
    }
    const timer = setInterval(() => {
      auctionApi
        .get(auctionId)
        .then(setAuction)
        .catch(() => {});
    }, 3000);
    return () => clearInterval(timer);
  }, [isEnded, auctionId]);

  if (loadError !== undefined) {
    return <p className="error">{loadError}</p>;
  }
  if (auction === undefined || view === undefined) {
    return <p>Loading…</p>;
  }

  const countdown = view.phase === "open" ? formatCountdown(view.endsAt, serverNowMs) : undefined;
  const minimumNextBid = view.hasBid
    ? addMinor(view.priceMinor, auction.minimumBidIncrementMinor)
    : view.priceMinor;
  const lines = live.log.filter(
    (line) => line.auctionId === auctionId || line.auctionId === undefined,
  );

  async function placeBid(): Promise<void> {
    const amountMinor = parseMajorToMinor(amount);
    if (amountMinor === undefined) {
      setBidFailed(true);
      setBidMessage("Enter an amount like 12.50.");
      return;
    }
    if (pendingBid.current?.amountMinor !== amountMinor) {
      pendingBid.current = { amountMinor, idempotencyKey: crypto.randomUUID() };
    }
    setBusy(true);
    setBidMessage(undefined);
    try {
      const accessToken = await auth.getAccessToken();
      if (accessToken === undefined) {
        setBidFailed(true);
        setBidMessage("Sign in to bid.");
        return;
      }
      const placed = await auctionApi.placeBid(
        accessToken,
        auctionId,
        amountMinor,
        pendingBid.current.idempotencyKey,
      );
      pendingBid.current = undefined;
      setBidFailed(false);
      setBidMessage(`Bid ${placed.bid.id} accepted at ${formatMinor(placed.bid.amountMinor)}.`);
    } catch (caught) {
      // A rejected bid keeps its key: sending the same amount again is a retry.
      setBidFailed(true);
      setBidMessage(describeError(caught));
    } finally {
      setBusy(false);
    }
  }

  return (
    <section>
      <h2>
        {auction.title}{" "}
        <span className={`status status-${live.status}`}>stream: {live.status}</span>
      </h2>
      <p>{auction.description}</p>
      <dl>
        <dt>Phase</dt>
        <dd>{view.phase}</dd>
        <dt>{view.hasBid ? "Leading bid" : "Starting price"}</dt>
        <dd className="price">{formatMoney(view.priceMinor, view.currencyCode)}</dd>
        <dt>Time left</dt>
        <dd>{countdown ?? "–"}</dd>
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
          {bidMessage !== undefined && <p className={bidFailed ? "error" : "ok"}>{bidMessage}</p>}
        </form>
      )}

      <h3>Stream log</h3>
      <ul className="log">
        {lines.map((line) => (
          <li key={line.id}>
            <time>{line.receivedAt}</time> {line.text}
          </li>
        ))}
      </ul>
    </section>
  );
}
