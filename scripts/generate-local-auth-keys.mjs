import { generateKeyPairSync } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const keyId = process.argv[2] ?? "local-2026-09";
if (!/^[A-Za-z0-9._-]{1,128}$/.test(keyId)) {
  throw new Error("Key ID must contain 1-128 letters, digits, dots, underscores, or hyphens");
}

const outputDirectory = resolve("services/identity-service/.local-keys");
const { privateKey, publicKey } = generateKeyPairSync("ec", {
  namedCurve: "prime256v1",
});
const privatePem = privateKey.export({ type: "pkcs8", format: "pem" });
const publicJwk = publicKey.export({ format: "jwk" });
const jwks = {
  keys: [{ ...publicJwk, alg: "ES256", kid: keyId, use: "sig" }],
};

await mkdir(outputDirectory, { recursive: true, mode: 0o700 });
await writeFile(resolve(outputDirectory, "private-key.pem"), privatePem, { mode: 0o600 });
await writeFile(resolve(outputDirectory, "jwks.json"), `${JSON.stringify(jwks, null, 2)}\n`, {
  mode: 0o644,
});
console.info(`Generated local ES256 signing material for kid ${keyId} in ${outputDirectory}`);
