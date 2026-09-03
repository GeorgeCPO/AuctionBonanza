import { buildApp } from "./app.js";
import { loadIdentityConfig } from "./config.js";
import { createDatabasePool } from "./database.js";
import { PasswordSecurity } from "./security/password-security.js";

if (import.meta.main) {
  const config = loadIdentityConfig();
  const database = createDatabasePool(config);
  const passwordSecurity = new PasswordSecurity();
  const app = buildApp({ config, database, passwordSecurity });

  const shutdown = async (signal: NodeJS.Signals): Promise<void> => {
    app.log.info({ signal }, "shutting down");
    await app.close();
    await database.end();
  };

  process.once("SIGINT", () => void shutdown("SIGINT"));
  process.once("SIGTERM", () => void shutdown("SIGTERM"));

  try {
    await app.listen({ host: config.host, port: config.port });
  } catch (error) {
    app.log.error(error);
    await database.end();
    process.exitCode = 1;
  }
}
