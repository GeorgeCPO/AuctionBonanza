import { describe, expect, it, vi } from "vitest";

import type { Database } from "../../src/database.js";
import { PostgresUserRepository } from "../../src/users/user.repository.js";

function makeDatabase(query: Database["query"]): Database {
  return {
    query,
    connect: async () => Promise.reject(new Error("unexpected transaction")),
  };
}

describe("PostgresUserRepository", () => {
  it("maps PostgreSQL login rows into a credential-specific model", async () => {
    const query = vi.fn<Database["query"]>(async () => ({
      rows: [
        {
          id: "42",
          email: "buyer@example.com",
          username: "Buyer",
          password_hash: "$argon2id$hash",
        },
      ],
    }));
    const repository = new PostgresUserRepository(makeDatabase(query));

    await expect(repository.findLoginIdentityByEmail("buyer@example.com")).resolves.toEqual({
      id: "42",
      email: "buyer@example.com",
      username: "Buyer",
      passwordHash: "$argon2id$hash",
    });
    expect(query).toHaveBeenCalledWith(expect.stringContaining("lower(email) = $1"), [
      "buyer@example.com",
    ]);
  });

  it("translates the email uniqueness constraint without leaking PostgreSQL details", async () => {
    const query = vi.fn<Database["query"]>(async () =>
      Promise.reject({
        code: "23505",
        constraint: "users_email_case_insensitive_key",
      }),
    );
    const repository = new PostgresUserRepository(makeDatabase(query));

    await expect(
      repository.create({
        email: "buyer@example.com",
        username: "Buyer",
        passwordHash: "$argon2id$hash",
      }),
    ).resolves.toEqual({ kind: "duplicate_email" });
  });

  it("does not translate unrelated database failures", async () => {
    const failure = { code: "23505", constraint: "users_username_case_insensitive_key" };
    const repository = new PostgresUserRepository(
      makeDatabase(async () => Promise.reject(failure)),
    );

    await expect(
      repository.create({
        email: "buyer@example.com",
        username: "Buyer",
        passwordHash: "$argon2id$hash",
      }),
    ).rejects.toBe(failure);
  });
});
