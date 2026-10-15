import type { SessionCredentials, UserJson } from "../api/api.types.js";

// The identity calls AuthSession needs. IdentityApi satisfies it; tests pass a
// fake.
export interface IdentityPort {
  register(email: string, password: string, username?: string): Promise<UserJson>;
  login(email: string, password: string): Promise<SessionCredentials>;
  refresh(refreshToken: string): Promise<SessionCredentials>;
  logout(accessToken: string, refreshToken: string): Promise<void>;
}

export interface AuthSessionOptions {
  identity: IdentityPort;
  // Milliseconds since the epoch; replaced in tests.
  now?: () => number;
  // Refresh this long before the access token expires.
  refreshLeewayMs?: number;
}

// What React reads. A new object is made on every change, so identity
// comparison tells a subscriber whether anything changed.
export interface AuthView {
  user: UserJson | null;
}
