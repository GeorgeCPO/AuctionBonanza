import { useLiveAuctions } from "../live/use-live-auctions.js";
import { formatMoney } from "../money.js";
import { auctionApi } from "../services.js";
import { viewAuction } from "./auction-view.js";
import { formatRoute } from "./route.js";
import { StreamStatusBadge } from "./StreamStatusBadge.js";
import { useLoaded } from "./use-loaded.js";
import { useServerNow } from "./use-server-now.js";

export function AuctionList() {
  const {
    data: auctions,
    error,
    reload,
  } = useLoaded(() => auctionApi.list().then((page) => page.items), []);

  const live = useLiveAuctions((auctions ?? []).map((auction) => auction.id));
  const serverNowMs = useServerNow(live.clockOffsetMs);

  return (
    <section>
      <h2>
        Auctions <StreamStatusBadge status={live.status} />{" "}
        <button type="button" onClick={reload}>
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
              return (
                <tr key={auction.id}>
                  <td>
                    <a href={formatRoute({ name: "auction", auctionId: auction.id })}>
                      {auction.title}
                    </a>
                  </td>
                  <td>{view.phase}</td>
                  <td>
                    {formatMoney(view.priceMinor, view.currencyCode)}
                    {!view.hasBid && <small> (starting)</small>}
                  </td>
                  <td>{view.countdown ?? "–"}</td>
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
