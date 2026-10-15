import { useSyncExternalStore } from "react";

import { AuctionDetail } from "./AuctionDetail.js";
import { AuctionList } from "./AuctionList.js";
import { AuthPanel } from "./AuthPanel.js";
import { CreateAuctionForm } from "./CreateAuctionForm.js";
import { formatRoute, parseRoute } from "./route.js";
import { useAuth } from "./use-auth.js";

function subscribeToHash(listener: () => void): () => void {
  window.addEventListener("hashchange", listener);
  return () => window.removeEventListener("hashchange", listener);
}

export function App() {
  const hash = useSyncExternalStore(subscribeToHash, () => window.location.hash);
  const route = parseRoute(hash);
  const { user } = useAuth();

  return (
    <>
      <header>
        <a href={formatRoute({ name: "list" })} className="brand">
          Auction Bonanza
        </a>
        <nav>{user !== null && <a href={formatRoute({ name: "new" })}>New auction</a>}</nav>
        <AuthPanel />
      </header>
      <main>
        {route.name === "list" && <AuctionList />}
        {route.name === "new" && <CreateAuctionForm />}
        {route.name === "auction" && (
          <AuctionDetail key={route.auctionId} auctionId={route.auctionId} />
        )}
      </main>
    </>
  );
}
