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
});
