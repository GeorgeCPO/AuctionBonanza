import type { SessionCredentials } from "../api/api.types.js";
import type { AuthSessionOptions, AuthView } from "./auth-session.types.js";

const DEFAULT_REFRESH_LEEWAY_MS = 30_000;

// The signed-in user's tokens. They live only in this object's memory: the
// identity service forbids localStorage for tokens, so a page reload signs the
// user out. Everything else asks `getAccessToken()` instead of holding a token.
export class AuthSession {
  private credentials: SessionCredentials | undefined;
  private accessExpiresAtMs = 0;
  // Refresh tokens are single use, so two callers that both see an expired
  // access token must share one refresh call instead of sending the same
  // token twice (the second would look like theft and revoke the session).
  private refreshInFlight: Promise<string | undefined> | undefined;
  private view: AuthView = { user: null };
  private readonly listeners = new Set<() => void>();
  private readonly now: () => number;
  private readonly refreshLeewayMs: number;

  constructor(private readonly options: AuthSessionOptions) {
    this.now = options.now ?? (() => Date.now());
    this.refreshLeewayMs = options.refreshLeewayMs ?? DEFAULT_REFRESH_LEEWAY_MS;
  }

  // Arrow properties so they can be handed to useSyncExternalStore as is.
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getView = (): AuthView => this.view;

  async login(email: string, password: string): Promise<void> {
    const credentials = await this.options.identity.login(email, password);
    this.setCredentials(credentials);
  }

  async register(email: string, password: string, username?: string): Promise<void> {
    await this.options.identity.register(email, password, username);
    await this.login(email, password);
  }

  async logout(): Promise<void> {
    const current = this.credentials;
    this.clear();
    if (current === undefined) {
      return;
    }
    // Best effort: the session is already gone locally either way.
    await this.options.identity.logout(current.accessToken, current.refreshToken).catch(() => {});
  }

  // Resolves to a token that is valid now, or undefined when there is no
  // session (never signed in, or the refresh failed and the user must sign in
  // again).
  async getAccessToken(): Promise<string | undefined> {
    const current = this.credentials;
    if (current === undefined) {
      return undefined;
    }
    const nowMs = this.now();
    if (nowMs < this.accessExpiresAtMs - this.refreshLeewayMs) {
      return current.accessToken;
    }
    this.refreshInFlight ??= this.refresh(current.refreshToken).finally(() => {
      this.refreshInFlight = undefined;
    });
    return this.refreshInFlight;
  }

  private async refresh(refreshToken: string): Promise<string | undefined> {
    try {
      const credentials = await this.options.identity.refresh(refreshToken);
      this.setCredentials(credentials);
      return credentials.accessToken;
    } catch {
      // The token may or may not have been consumed (a lost response looks the
      // same as a rejection), so the only safe recovery is a fresh login.
      this.clear();
      return undefined;
    }
  }

  private setCredentials(credentials: SessionCredentials): void {
    this.credentials = credentials;
    this.accessExpiresAtMs = this.now() + credentials.expiresIn * 1000;
    this.publish({ user: credentials.user });
  }

  private clear(): void {
    this.credentials = undefined;
    this.accessExpiresAtMs = 0;
    this.publish({ user: null });
  }

  private publish(view: AuthView): void {
    this.view = view;
    for (const listener of this.listeners) {
      listener();
    }
  }
}
