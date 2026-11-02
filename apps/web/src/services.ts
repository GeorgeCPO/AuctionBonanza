import { AuctionApi } from "./api/auction.api.js";
import { IdentityApi } from "./api/identity.api.js";
import { AuthSession } from "./auth/auth-session.js";

// The app's one session and one auction client, shared by every component.
export const auth = new AuthSession({ identity: new IdentityApi() });
export const auctionApi = new AuctionApi();
