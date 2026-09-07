import { buildApp } from "./app.js";
import { createAuctionAccessTokenVerifier, createAuthenticationHook } from "./authentication.js";
import { PostgresAuctionRepository } from "./auctions/auction.repository.js";
import { AuctionService } from "./auctions/auction.service.js";
import { loadAuctionConfig } from "./config.js";
import { createDatabasePool } from "./database.js";

if (import.meta.main) {
  const config = loadAuctionConfig();
  const database = createDatabasePool(config);
  const accessTokenVerifier = createAuctionAccessTokenVerifier({
    issuer: config.authIssuer,
    audience: config.authAudience,
    jwksUrl: config.authJwksUrl,
    accessTokenTtlSeconds: config.accessTokenTtlSeconds,
    clockSkewSeconds: config.accessTokenClockSkewSeconds,
    jwksCacheTtlMilliseconds: config.authJwksCacheTtlMilliseconds,
  });
  const auctions = new AuctionService(
    new PostgresAuctionRepository(database),
    config.supportedCurrencyCodes,
  );
  const app = buildApp({
    config,
    database,
    auctions,
    authenticate: createAuthenticationHook(accessTokenVerifier),
  });

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
