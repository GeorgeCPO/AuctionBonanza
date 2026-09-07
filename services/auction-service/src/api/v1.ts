import type { ApiTree } from "@auction-bonanza/http-routing";

import { AuctionController } from "../auctions/auction.controller.js";
import type { AuctionApiDependencies } from "./dependencies.js";

export function createV1Api(dependencies: AuctionApiDependencies): ApiTree {
  const auctions = new AuctionController(dependencies);

  return {
    v1: {
      auctions: {
        post: auctions.create,
        ":id": {
          get: auctions.findById,
        },
      },
    },
  };
}
