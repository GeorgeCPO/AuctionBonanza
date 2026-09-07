import type { IdentityConfig } from "../config.js";
import type { Database } from "../database.js";
import type { AccessTokens } from "../security/access-tokens.js";
import type { PasswordSecurityPort } from "../security/password-security.js";
import type { SessionService } from "../sessions/session.service.js";
import type { UserService } from "../users/user.service.js";

export type { PasswordSecurityPort } from "../security/password-security.js";

export interface IdentityApiDependencies {
  config: IdentityConfig;
  database: Database;
  passwordSecurity: PasswordSecurityPort;
  accessTokens: AccessTokens;
}

export interface IdentityRouteDependencies {
  config: IdentityConfig;
  database: Database;
  accessTokens: AccessTokens;
  sessions: SessionService;
  users: UserService;
}
