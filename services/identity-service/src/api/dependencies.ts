import type { IdentityConfig } from "../config.js";
import type { Database } from "../database.js";
import type { PasswordSecurity } from "../security/password-security.js";

export interface IdentityApiDependencies {
  config: IdentityConfig;
  database: Database;
  passwordSecurity: PasswordSecurity;
}
