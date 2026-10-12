import { generateKeyPairSync, randomUUID, sign } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import type { TestIssuer } from "./test-issuer.types.js";

// Stands in for identity-service: serves a JWKS with one ES256 key and signs
// access tokens with the claims auction-service verifies.
export async function startTestIssuer(audience = "auction-api"): Promise<TestIssuer> {
  const { privateKey, publicKey } = generateKeyPairSync("ec", { namedCurve: "P-256" });
  const kid = randomUUID();
  const jwks = {
    keys: [{ ...publicKey.export({ format: "jwk" }), kid, alg: "ES256", use: "sig" }],
  };
  const server = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify(jwks));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const issuer = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

  return {
    issuer,
    audience,
    jwksUrl: `${issuer}/.well-known/jwks.json`,
    accessToken: (userId) => {
      const issuedAt = Math.floor(Date.now() / 1000);
      const header = { alg: "ES256", typ: "at+jwt", kid };
      const payload = {
        iss: issuer,
        aud: audience,
        sub: userId,
        sid: randomUUID(),
        jti: randomUUID(),
        iat: issuedAt,
        exp: issuedAt + 300,
      };
      const signingInput = `${base64url(header)}.${base64url(payload)}`;
      const signature = sign("sha256", Buffer.from(signingInput), {
        key: privateKey,
        dsaEncoding: "ieee-p1363",
      });
      return `${signingInput}.${signature.toString("base64url")}`;
    },
    stop: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

function base64url(value: unknown): string {
  return Buffer.from(JSON.stringify(value)).toString("base64url");
}
