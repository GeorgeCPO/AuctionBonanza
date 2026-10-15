import { useEffect, useState } from "react";

import type { AuctionJson } from "../api/api.types.js";
import { describeError } from "../error-messages.js";
import { useLiveAuctions } from "../live/use-live-auctions.js";
import { formatCountdown } from "../live/phase.js";
import { formatMoney } from "../money.js";
import { auctionApi } from "../services.js";
import { viewAuction } from "./auction-view.js";
import { useServerNow } from "./use-server-now.js";

export function AuctionList() {
  const [auctions, setAuctions] = useState<AuctionJson[] | undefined>();
  const [error, setError] = useState<string | undefined>();
  const [reloads, setReloads] = useState(0);

  useEffect(() => {
    let cancelled = false;
    auctionApi
      .list(20)
      .then((page) => {
        if (!cancelled) {
          setAuctions(page.items);
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
  }, [reloads]);

  const live = useLiveAuctions((auctions ?? []).map((auction) => auction.id));
  const serverNowMs = useServerNow(live.clockOffsetMs);

  return (
    <section>
      <h2>
        Auctions <span className={`status status-${live.status}`}>stream: {live.status}</span>{" "}
        <button type="button" onClick={() => setReloads((count) => count + 1)}>
          Reload list
        </button>
      </h2>
      {error !== undefined && <p className="error">{error}</p>}
      {auctions === undefined && error === undefined && <p>Loading…</p>}
      {auctions?.length === 0 && <p>No auctions yet. Sign in and create one.</p>}
      {auctions !== undefined && auctions.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Title</th>
              <th>Phase</th>
              <th>Price</th>
              <th>Time left</th>
              <th>Version</th>
            </tr>
          </thead>
          <tbody>
            {auctions.map((auction) => {
              const view = viewAuction(auction, live.entries.get(auction.id), serverNowMs);
              const countdown =
                view.phase === "open" ? formatCountdown(view.endsAt, serverNowMs) : undefined;
              return (
                <tr key={auction.id}>
                  <td>
                    <a href={`#/auctions/${auction.id}`}>{auction.title}</a>
                  </td>
                  <td>{view.phase}</td>
                  <td>
                    {formatMoney(view.priceMinor, view.currencyCode)}
                    {!view.hasBid && <small> (starting)</small>}
                  </td>
                  <td>{countdown ?? "–"}</td>
                  <td>{view.version ?? "–"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </section>
  );
}
