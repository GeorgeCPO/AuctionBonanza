import { argon2, randomBytes } from "node:crypto";

const ARGON2_VERSION = 19;
const MEMORY_KIB = 19_456;
const PASSES = 2;
const PARALLELISM = 1;
const TAG_LENGTH = 32;

export class PasswordSecurity {
  async hash(password: string): Promise<string> {
    const salt = randomBytes(16);
    const derivedKey = await new Promise<Buffer>((resolve, reject) => {
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

    const encodedSalt = salt.toString("base64").replaceAll("=", "");
    const encodedKey = derivedKey.toString("base64").replaceAll("=", "");

    return `$argon2id$v=${ARGON2_VERSION}$m=${MEMORY_KIB},t=${PASSES},p=${PARALLELISM}$${encodedSalt}$${encodedKey}`;
  }
}
