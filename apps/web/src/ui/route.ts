import type { Route } from "./route.types.js";

// Hash routing: "#/", "#/new", "#/auctions/42". No router library needed for
// three screens.
export function parseRoute(hash: string): Route {
  if (hash === "#/new") {
    return { name: "new" };
  }
  const match = /^#\/auctions\/(\d+)$/.exec(hash);
  if (match?.[1] !== undefined) {
    return { name: "auction", auctionId: match[1] };
  }
  return { name: "list" };
}
