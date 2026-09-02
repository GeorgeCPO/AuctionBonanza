import { loadIdentityConfig } from "./config.js";

export { loadIdentityConfig, parseIdentityConfig } from "./config.js";
export type { IdentityConfig } from "./config.js";

if (import.meta.main) {
  const config = loadIdentityConfig();

  console.info(
    `identity-service configuration valid (${config.nodeEnv}, ${config.host}:${config.port})`,
  );
}
