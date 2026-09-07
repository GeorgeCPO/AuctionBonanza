import { describe, expect, it } from "vitest";

import { PasswordSecurity } from "../src/security/password-security.js";

describe("PasswordSecurity", () => {
  it("creates salted Argon2id PHC strings", async () => {
    const security = new PasswordSecurity();
    const first = await security.hash("a strong password");
    const second = await security.hash("a strong password");

    expect(first).toMatch(/^\$argon2id\$v=19\$m=19456,t=2,p=1\$[^$]+\$[^$]+$/);
    expect(second).toMatch(/^\$argon2id\$/);
    expect(second).not.toBe(first);
  });

  it("verifies valid passwords and rejects incorrect passwords in constant-time comparison", async () => {
    const security = new PasswordSecurity();
    const hash = await security.hash("a strong password");

    await expect(security.verify("a strong password", hash)).resolves.toBe(true);
    await expect(security.verify("a different password", hash)).resolves.toBe(false);
  });

  it.each([
    "not-a-phc-string",
    "$argon2i$v=19$m=19456,t=2,p=1$YXVjdGlvbi1kdW1teS0wMQ$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "$argon2id$v=19$m=1,t=1,p=1$YXVjdGlvbi1kdW1teS0wMQ$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  ])("fails closed for malformed or unsupported hash %s", async (hash) => {
    const security = new PasswordSecurity();
    await expect(security.verify("a strong password", hash)).resolves.toBe(false);
  });

  it("performs the dummy verification path for an unknown user", async () => {
    const security = new PasswordSecurity();
    await expect(security.verifyUnknownUser("candidate password")).resolves.toBeUndefined();
  });
});
