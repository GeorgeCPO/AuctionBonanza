import { argon2, randomBytes, timingSafeEqual } from "node:crypto";

const ARGON2_VERSION = 19;
const MEMORY_KIB = 19_456;
const PASSES = 2;
const PARALLELISM = 1;
const TAG_LENGTH = 32;
const SALT_LENGTH = 16;
const PHC_PATTERN = /^\$argon2id\$v=19\$m=19456,t=2,p=1\$([A-Za-z0-9+/]{22})\$([A-Za-z0-9+/]{43})$/;

// This is a public, non-user hash used only to ensure unknown-email logins still
// perform one full Argon2id derivation. Its plaintext is intentionally irrelevant.
const UNKNOWN_USER_HASH =
  "$argon2id$v=19$m=19456,t=2,p=1$YXVjdGlvbi1kdW1teS0wMQ$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

export interface PasswordSecurityPort {
  hash(password: string): Promise<string>;
  verify(password: string, passwordHash: string): Promise<boolean>;
  verifyUnknownUser(password: string): Promise<void>;
}

export class PasswordSecurity implements PasswordSecurityPort {
  async hash(password: string): Promise<string> {
    const salt = randomBytes(SALT_LENGTH);
    const derivedKey = await this.derive(password, salt);

    const encodedSalt = salt.toString("base64").replaceAll("=", "");
    const encodedKey = derivedKey.toString("base64").replaceAll("=", "");

    return `$argon2id$v=${ARGON2_VERSION}$m=${MEMORY_KIB},t=${PASSES},p=${PARALLELISM}$${encodedSalt}$${encodedKey}`;
  }

  async verify(password: string, passwordHash: string): Promise<boolean> {
    const parsed = PHC_PATTERN.exec(passwordHash);
    if (parsed?.[1] === undefined || parsed[2] === undefined) {
      await this.performDummyVerification(password);
      return false;
    }

    const salt = Buffer.from(parsed[1], "base64");
    const expected = Buffer.from(parsed[2], "base64");
    if (salt.length !== SALT_LENGTH || expected.length !== TAG_LENGTH) {
      await this.performDummyVerification(password);
      return false;
    }

    const actual = await this.derive(password, salt);
    return timingSafeEqual(actual, expected);
  }

  async verifyUnknownUser(password: string): Promise<void> {
    await this.verify(password, UNKNOWN_USER_HASH);
  }

  private async performDummyVerification(password: string): Promise<void> {
    const parsed = PHC_PATTERN.exec(UNKNOWN_USER_HASH);
    if (parsed?.[1] === undefined || parsed[2] === undefined) {
      throw new Error("Invalid built-in unknown-user password hash");
    }
    const actual = await this.derive(password, Buffer.from(parsed[1], "base64"));
    timingSafeEqual(actual, Buffer.from(parsed[2], "base64"));
  }

  private async derive(password: string, salt: Buffer): Promise<Buffer> {
    return new Promise<Buffer>((resolve, reject) => {
      argon2(
        "argon2id",
        {
          message: Buffer.from(password, "utf8"),
          nonce: salt,
          parallelism: PARALLELISM,
          tagLength: TAG_LENGTH,
          memory: MEMORY_KIB,
          passes: PASSES,
        },
        (error, key) => {
          if (error) {
            reject(error);
            return;
          }

          resolve(key);
        },
      );
    });
  }
}
