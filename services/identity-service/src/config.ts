import { Type, type Static } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

const identityConfigSchema = Type.Object(
  {
    nodeEnv: Type.Union([
      Type.Literal("development"),
      Type.Literal("test"),
      Type.Literal("production"),
    ]),
    host: Type.String({ minLength: 1 }),
    port: Type.Integer({ minimum: 1, maximum: 65_535 }),
    databaseUrl: Type.String({
      pattern: "^postgres(?:ql)?://.+",
    }),
  },
  { additionalProperties: false },
);

export type IdentityConfig = Readonly<Static<typeof identityConfigSchema>>;

export function parseIdentityConfig(
  environment: Readonly<Record<string, string | undefined>>,
): IdentityConfig {
  const candidate = {
    nodeEnv: environment.NODE_ENV ?? "development",
    host: environment.HOST ?? "0.0.0.0",
    port: Number(environment.PORT ?? "3001"),
    databaseUrl: environment.DATABASE_URL,
  };

  if (!Value.Check(identityConfigSchema, candidate)) {
    const details = [...Value.Errors(identityConfigSchema, candidate)]
      .map((error) => `${error.path || "/"}: ${error.message}`)
      .join("; ");

    throw new Error(`Invalid identity-service configuration: ${details}`);
  }

  // TypeBox performs the runtime narrowing above; TypeScript cannot infer it
  // through Value.Check, so this assertion joins the runtime and static types.
  return candidate as IdentityConfig;
}

export function loadIdentityConfig(): IdentityConfig {
  return parseIdentityConfig(process.env);
}
