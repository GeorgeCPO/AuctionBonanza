import type { SessionCredentials, UserJson } from "./api.types.js";
import { HttpClient } from "./http.js";

// The identity-service calls the UI uses. `/identity` is the dev-server
// prefix for port 3001.
export class IdentityApi {
  constructor(private readonly http: HttpClient = new HttpClient("/identity")) {}

  register(email: string, password: string, username?: string): Promise<UserJson> {
    const body =
      username === undefined || username === ""
        ? { email, password }
        : { email, password, username };
    return this.http.request<UserJson>("/v1/users", { method: "POST", body });
  }

  login(email: string, password: string): Promise<SessionCredentials> {
    return this.http.request<SessionCredentials>("/v1/sessions", {
      method: "POST",
      body: { email, password },
    });
  }

  // Refresh tokens are single use: a successful call consumes the one sent.
  refresh(refreshToken: string): Promise<SessionCredentials> {
    return this.http.request<SessionCredentials>("/v1/sessions/refresh", {
      method: "POST",
      body: { refreshToken },
    });
  }

  logout(accessToken: string, refreshToken: string): Promise<void> {
    return this.http.request<void>("/v1/sessions/current", {
      method: "DELETE",
      body: { refreshToken },
      accessToken,
    });
  }
}
