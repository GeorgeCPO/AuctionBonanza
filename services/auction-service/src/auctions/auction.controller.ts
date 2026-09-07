import { Type } from "@sinclair/typebox";
import type { ApiRoute } from "@auction-bonanza/http-routing";

import type { AuctionApiDependencies } from "../api/dependencies.js";
import { authenticatedRoute } from "../api/authenticated-route.js";
import { isPostgresBigintId, type CreateAuctionInput } from "./auction.service.js";
import { toAuctionJson } from "./auction.js";

const timestampSchema = Type.String({ format: "date-time", maxLength: 35 });
const creationBodySchema = Type.Object(
  {
    title: Type.String({ minLength: 1, maxLength: 512, pattern: "\\S" }),
    description: Type.String({ minLength: 1, maxLength: 10_240, pattern: "\\S" }),
    currencyCode: Type.String({ pattern: "^[A-Z]{3}$" }),
    startsAt: Type.Optional(timestampSchema),
    endsAt: timestampSchema,
    startingPriceMinor: Type.String({ pattern: "^(0|[1-9][0-9]{0,18})$" }),
    minimumBidIncrementMinor: Type.String({ pattern: "^[1-9][0-9]{0,18}$" }),
  },
  { additionalProperties: false },
);

const auctionIdParamsSchema = Type.Object(
  { id: Type.String({ pattern: "^[1-9][0-9]{0,18}$" }) },
  { additionalProperties: false },
);

export class AuctionController {
  constructor(private readonly dependencies: AuctionApiDependencies) {}

  get create(): ApiRoute {
    return authenticatedRoute({
      authenticate: this.dependencies.authenticate,
      schema: {
        body: creationBodySchema,
      },
      handler: async (request, reply, { principal }) => {
        const outcome = await this.dependencies.auctions.create(
          principal.userId,
          request.body as CreateAuctionInput,
        );
        if (outcome.kind === "invalid") {
          return reply.code(400).send({ code: "INVALID_AUCTION", message: outcome.message });
        }
        reply.header("location", `/v1/auctions/${outcome.auction.id}`);
        return reply.code(201).send(toAuctionJson(outcome.auction));
      },
    });
  }

  get findById(): ApiRoute {
    return {
      schema: { params: auctionIdParamsSchema },
      handler: async (request, reply) => {
        const { id } = request.params as { id: string };
        if (!isPostgresBigintId(id)) {
          return reply.code(400).send({
            code: "INVALID_REQUEST",
            message: "Request validation failed",
          });
        }

        const auction = await this.dependencies.auctions.findById(id);
        if (auction === undefined) {
          return reply.code(404).send({
            code: "AUCTION_NOT_FOUND",
            message: "Auction was not found",
          });
        }
        return toAuctionJson(auction);
      },
    } satisfies ApiRoute;
  }
}
