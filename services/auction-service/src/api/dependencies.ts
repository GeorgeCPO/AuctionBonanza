import type { preHandlerAsyncHookHandler } from "fastify";

import type { AuctionServicePort } from "../auctions/auction.service.js";
import type { BidServicePort } from "../bids/bid.service.js";
import type { AuctionConfig } from "../config.js";
import type { Database } from "../database.js";

export interface AuctionApiDependencies {
  config: AuctionConfig;
  database: Database;
  auctions: AuctionServicePort;
  bids: BidServicePort;
  authenticate: preHandlerAsyncHookHandler;
}
