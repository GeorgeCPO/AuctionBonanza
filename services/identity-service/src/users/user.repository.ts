import type { Database } from "../database.js";

export interface UserIdentity {
  id: string;
  email: string;
  username: string | null;
}

export interface LoginIdentity extends UserIdentity {
  passwordHash: string;
}

export interface RegisteredUser extends UserIdentity {
  createdAt: Date;
}

export interface CreateUserInput {
  email: string;
  username: string | null;
  passwordHash: string;
}

export type CreateUserOutcome =
  | { kind: "created"; user: RegisteredUser }
  | { kind: "duplicate_email" };

export interface UserRepository {
  findLoginIdentityByEmail(normalizedEmail: string): Promise<LoginIdentity | undefined>;
  create(input: CreateUserInput): Promise<CreateUserOutcome>;
}

interface LoginIdentityRow extends Record<string, unknown> {
  id: string;
  email: string;
  username: string | null;
  password_hash: string;
}

interface RegisteredUserRow extends Record<string, unknown> {
  id: string;
  email: string;
  username: string | null;
  created_at: Date;
}

interface PostgreSqlError {
  code?: string;
  constraint?: string;
}

export class PostgresUserRepository implements UserRepository {
  constructor(private readonly database: Database) {}

  async findLoginIdentityByEmail(normalizedEmail: string): Promise<LoginIdentity | undefined> {
    const result = await this.database.query(
      `SELECT id::text, email, username, password_hash
       FROM users
       WHERE lower(email) = $1
       LIMIT 1`,
      [normalizedEmail],
    );
    const row = result.rows[0] as LoginIdentityRow | undefined;

    return row === undefined
      ? undefined
      : {
          id: row.id,
          email: row.email,
          username: row.username,
          passwordHash: row.password_hash,
        };
  }

  async create(input: CreateUserInput): Promise<CreateUserOutcome> {
    try {
      const result = await this.database.query(
        `INSERT INTO users (email, username, password_hash)
         VALUES ($1, $2, $3)
         RETURNING id::text, email, username, created_at`,
        [input.email, input.username, input.passwordHash],
      );
      const row = result.rows[0] as RegisteredUserRow | undefined;

      if (row === undefined) {
        throw new Error("User insert returned no row");
      }

      return {
        kind: "created",
        user: {
          id: row.id,
          email: row.email,
          username: row.username,
          createdAt: row.created_at,
        },
      };
    } catch (error) {
      if (isDuplicateEmail(error)) {
        return { kind: "duplicate_email" };
      }
      throw error;
    }
  }
}

function isDuplicateEmail(error: unknown): boolean {
  if (typeof error !== "object" || error === null) {
    return false;
  }

  const postgresError = error as PostgreSqlError;
  return (
    postgresError.code === "23505" &&
    postgresError.constraint === "users_email_case_insensitive_key"
  );
}
