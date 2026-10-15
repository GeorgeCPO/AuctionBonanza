import type {
  AuctionJson,
  AuctionListJson,
  CreateAuctionRequest,
  LiveStatesJson,
  PlaceBidResponse,
} from "./api.types.js";
import { HttpClient } from "./http.js";

// The auction-service calls the UI uses. `/auction` is the dev-server prefix
// for port 3002. Calls that need a user take the access token as an argument,
// so this class holds no session state.
export class AuctionApi {
  constructor(private readonly http: HttpClient = new HttpClient("/auction")) {}

  list(limit = 20): Promise<AuctionListJson> {
    return this.http.request<AuctionListJson>(`/v1/auctions?limit=${limit}`);
  }

  get(auctionId: string): Promise<AuctionJson> {
    return this.http.request<AuctionJson>(`/v1/auctions/${encodeURIComponent(auctionId)}`);
  }

  liveStates(auctionIds: readonly string[]): Promise<LiveStatesJson> {
    const ids = auctionIds.map(encodeURIComponent).join(",");
    return this.http.request<LiveStatesJson>(`/v1/auctions/live-states?ids=${ids}`);
  }

  create(accessToken: string, request: CreateAuctionRequest): Promise<AuctionJson> {
    return this.http.request<AuctionJson>("/v1/auctions", {
      method: "POST",
      body: request,
      accessToken,
    });
  }

  // The same idempotency key makes a retry return the first result instead of
  // placing a second bid.
  placeBid(
    accessToken: string,
    auctionId: string,
    amountMinor: string,
    idempotencyKey: string,
  ): Promise<PlaceBidResponse> {
    return this.http.request<PlaceBidResponse>(
      `/v1/auctions/${encodeURIComponent(auctionId)}/bids`,
      { method: "POST", body: { amountMinor, idempotencyKey }, accessToken },
    );
  }
}
