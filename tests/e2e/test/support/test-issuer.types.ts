export interface TestIssuer {
  issuer: string;
  audience: string;
  jwksUrl: string;
  // A signed access token for the user, as identity-service would issue it.
  accessToken(userId: string): string;
  stop(): Promise<void>;
}
