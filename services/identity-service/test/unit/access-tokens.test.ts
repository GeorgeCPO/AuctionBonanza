import { generateKeyPairSync } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { decodeJwt, decodeProtectedHeader } from "jose";
import { afterEach, describe, expect, it } from "vitest";

import type { IdentityConfig } from "../../src/config.js";
import { createAccessTokens } from "../../src/security/access-tokens.js";

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map(async (directory) => rm(directory, { recursive: true })),
  );
});

async function createKeyConfiguration(keyId = "test-key"): Promise<IdentityConfig> {
  const directory = await mkdtemp(join(tmpdir(), "auction-auth-key-test-"));
  temporaryDirectories.push(directory);
  const { privateKey, publicKey } = generateKeyPairSync("ec", { namedCurve: "prime256v1" });
  const privateKeyPath = join(directory, "private.pem");
  const jwksPath = join(directory, "jwks.json");
  await writeFile(privateKeyPath, privateKey.export({ type: "pkcs8", format: "pem" }));
  await writeFile(
    jwksPath,
    JSON.stringify({
      keys: [
        {
          ...publicKey.export({ format: "jwk" }),
          alg: "ES256",
          kid: keyId,
          use: "sig",
        },
      ],
    }),
  );

  return {
    nodeEnv: "test",
    host: "127.0.0.1",
    port: 3001,
    databaseUrl: "postgresql://unused",
    authIssuer: "https://identity.test",
    authAudience: "auction-api",
    accessTokenTtlSeconds: 600,
    refreshSessionTtlSeconds: 2_592_000,
    accessTokenClockSkewSeconds: 5,
    signingAlgorithm: "ES256",
    activeSigningKeyId: keyId,
    signingPrivateKeyPath: privateKeyPath,
    signingJwksPath: jwksPath,
  };
}

describe("identity access-token signing", () => {
  it("issues the minimal strict token and verifies it through the shared verifier", async () => {
    const accessTokens = await createAccessTokens(await createKeyConfiguration());
    const token = await accessTokens.issue("42", "00000000-0000-4000-8000-000000000001");

    expect(decodeProtectedHeader(token)).toEqual({
      alg: "ES256",
      kid: "test-key",
      typ: "at+jwt",
    });
    const payload = decodeJwt(token);
    expect(Object.keys(payload).sort()).toEqual(["aud", "exp", "iat", "iss", "jti", "sid", "sub"]);
    expect(payload.exp! - payload.iat!).toBe(600);
    await expect(accessTokens.verify(token)).resolves.toMatchObject({
      userId: "42",
      sessionId: "00000000-0000-4000-8000-000000000001",
    });
    expect(accessTokens.publicJwks().keys[0]).not.toHaveProperty("d");
  });

  it("refuses to start when the active private key and JWKS do not match", async () => {
    const first = await createKeyConfiguration("shared-kid");
    const second = await createKeyConfiguration("shared-kid");
    await expect(
      createAccessTokens({ ...first, signingJwksPath: second.signingJwksPath }),
    ).rejects.toThrow("does not match");
  });
});
