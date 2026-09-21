import type { ApiTree } from "@auction-bonanza/http-routing";

import { AuctionController } from "../auctions/auction.controller.js";
import { BidController } from "../bids/bid.controller.js";
import type { AuctionApiDependencies } from "./dependencies.js";

export function createV1Api(dependencies: AuctionApiDependencies): ApiTree {
  const auctions = new AuctionController(dependencies);
  const bids = new BidController(dependencies);

  return {
    v1: {
      auctions: {
        get: auctions.list,
        post: auctions.create,
        ":id": {
          get: auctions.findById,
          bids: { post: bids.create },
        },
      },
    },
  };
}
