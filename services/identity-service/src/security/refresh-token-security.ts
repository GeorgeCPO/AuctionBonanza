import { createHash, randomBytes } from "node:crypto";

const REFRESH_TOKEN_BYTES = 32;
const REFRESH_TOKEN_PATTERN = /^[A-Za-z0-9_-]{43}$/;

export class RefreshTokenSecurity {
  generate(): string {
    return randomBytes(REFRESH_TOKEN_BYTES).toString("base64url");
  }

  digest(token: string): Buffer {
    return createHash("sha256").update(token, "utf8").digest();
  }

  isWellFormed(token: string): boolean {
    return REFRESH_TOKEN_PATTERN.test(token);
  }
}
