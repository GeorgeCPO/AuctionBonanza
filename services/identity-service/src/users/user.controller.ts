import { Type } from "@sinclair/typebox";
import type { ApiRoute } from "@auction-bonanza/http-routing";

import type { IdentityRouteDependencies } from "../api/dependencies.js";

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

export class UserController {
  constructor(private readonly dependencies: IdentityRouteDependencies) {}

  readonly register: ApiRoute = {
    schema: { body: registrationBodySchema },
    handler: async (request, reply) => {
      const body = request.body as RegistrationBody;
      const outcome = await this.dependencies.users.register(body);
      if (outcome.kind === "duplicate_email") {
        return reply.code(409).send({
          code: "EMAIL_ALREADY_REGISTERED",
          message: "A user with this email already exists",
        });
      }

      return reply.code(201).send({
        id: outcome.user.id,
        email: outcome.user.email,
        username: outcome.user.username,
        createdAt: outcome.user.createdAt.toISOString(),
      });
    },
  } satisfies ApiRoute;
}
