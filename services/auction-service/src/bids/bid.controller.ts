import { Type } from "@sinclair/typebox";
import type { ApiRoute } from "@auction-bonanza/http-routing";

import type { AuctionApiDependencies } from "../api/dependencies.js";
import { authenticatedRoute } from "../api/authenticated-route.js";
import { isPostgresBigintId } from "../shared/money.js";
import { toBidJson } from "./bid.js";
import { MAX_IDEMPOTENCY_KEY_LENGTH, type PlaceBidInput } from "./bid.service.js";

const auctionIdParamsSchema = Type.Object(
  { id: Type.String({ pattern: "^[1-9][0-9]{0,18}$" }) },
  { additionalProperties: false },
);

const placeBidBodySchema = Type.Object(
  {
    amountMinor: Type.String({ pattern: "^[1-9][0-9]{0,18}$" }),
    idempotencyKey: Type.String({
      minLength: 1,
      maxLength: MAX_IDEMPOTENCY_KEY_LENGTH,
      pattern: "^\\S+$",
    }),
  },
  { additionalProperties: false },
);

export class BidController {
  constructor(private readonly dependencies: AuctionApiDependencies) {}

  get create(): ApiRoute {
    return authenticatedRoute({
      authenticate: this.dependencies.authenticate,
      schema: {
        params: auctionIdParamsSchema,
        body: placeBidBodySchema,
      },
      handler: async (request, reply, { principal }) => {
        const { id } = request.params as { id: string };
        // The params pattern bounds the digit count, not the value: a 19-digit
        // ID above bigint's maximum would otherwise reach PostgreSQL as an
        // out-of-range cast (22003) and surface as a 500.
        if (!isPostgresBigintId(id)) {
          return reply.code(400).send({
            code: "INVALID_REQUEST",
            message: "Request validation failed",
          });
        }

        const outcome = await this.dependencies.bids.create(
          id,
          principal.userId,
          request.body as PlaceBidInput,
        );

        switch (outcome.kind) {
          case "invalid":
            return reply.code(400).send({ code: "INVALID_BID", message: outcome.message });
          case "auction_not_found":
            return reply.code(404).send({
              code: "AUCTION_NOT_FOUND",
              message: "Auction was not found",
            });
          case "auction_not_open":
            return reply.code(409).send({
              code: "AUCTION_NOT_OPEN",
              message: "Auction is not currently open for bidding",
              phase: outcome.phase,
            });
          case "self_bid_not_allowed":
            return reply.code(403).send({
              code: "SELF_BID_NOT_ALLOWED",
              message: "The auction owner cannot bid on their own auction",
            });
          case "bid_too_low":
            return reply.code(422).send({
              code: "BID_TOO_LOW",
              message: "Bid does not meet the minimum qualifying amount",
              minimumQualifyingAmountMinor: outcome.minimumQualifyingAmountMinor,
            });
          case "idempotency_key_reused":
            return reply.code(409).send({
              code: "IDEMPOTENCY_KEY_REUSED",
              message: "This idempotency key was already used with a different amount",
              originalAmountMinor: outcome.originalAmountMinor,
            });
          case "placed":
            return reply.code(outcome.replay ? 200 : 201).send({
              bid: toBidJson(outcome.bid),
              auction: {
                id: outcome.auction.id,
                version: outcome.auction.version,
                leadingBidId: outcome.auction.leadingBidId,
                updatedAt: outcome.auction.updatedAt.toISOString(),
              },
            });
        }
      },
    });
  }
}
