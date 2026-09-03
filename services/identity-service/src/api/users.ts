import { Type } from "@sinclair/typebox";
import type { ApiRoute } from "@auction-bonanza/http-routing";

import type { IdentityApiDependencies } from "./dependencies.js";

const registrationBodySchema = Type.Object(
  {
    email: Type.String({
      minLength: 3,
      maxLength: 320,
      pattern: "^\\s*[^\\s@]+@[^\\s@]+\\.[^\\s@]+\\s*$",
    }),
    password: Type.String({ minLength: 12, maxLength: 1_024 }),
    username: Type.Optional(Type.String({ minLength: 1, maxLength: 100, pattern: ".*\\S.*" })),
  },
  { additionalProperties: false },
);

interface RegistrationBody {
  email: string;
  password: string;
  username?: string;
}

interface UserRow extends Record<string, unknown> {
  id: string;
  email: string;
  username: string | null;
  created_at: Date;
}

interface PostgreSqlError {
  code?: string;
  constraint?: string;
}

export class Users {
  constructor(private readonly dependencies: IdentityApiDependencies) {}

  readonly register: ApiRoute = {
    schema: { body: registrationBodySchema },
    handler: async (request, reply) => {
      const body = request.body as RegistrationBody;
      const email = body.email.trim().toLowerCase();
      const username = body.username?.trim() ?? null;
      const passwordHash = await this.dependencies.passwordSecurity.hash(body.password);

      try {
        const result = await this.dependencies.database.query(
          `INSERT INTO users (email, username, password_hash)
           VALUES ($1, $2, $3)
           RETURNING id::text, email, username, created_at`,
          [email, username, passwordHash],
        );
        const user = result.rows[0] as UserRow | undefined;

        if (user === undefined) {
          throw new Error("User insert returned no row");
        }

        return reply.code(201).send({
          id: user.id,
          email: user.email,
          username: user.username,
          createdAt: user.created_at.toISOString(),
        });
      } catch (error) {
        if (this.isDuplicateEmail(error)) {
          return reply.code(409).send({
            code: "EMAIL_ALREADY_REGISTERED",
            message: "A user with this email already exists",
          });
        }

        throw error;
      }
    },
  } satisfies ApiRoute;

  private isDuplicateEmail(error: unknown): boolean {
    if (typeof error !== "object" || error === null) {
      return false;
    }

    const postgresError = error as PostgreSqlError;
    return (
      postgresError.code === "23505" &&
      postgresError.constraint === "users_email_case_insensitive_key"
    );
  }
}
