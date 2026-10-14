package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/token"
)

func runToken(args []string) int {
	flags := flag.NewFlagSet("token", flag.ContinueOnError)
	user := flags.String("user", "", "user id the token is for (required)")
	keyPath := flags.String("key", environmentOr("AUTH_SIGNING_PRIVATE_KEY_PATH", "services/identity-service/.local-keys/private-key.pem"), "ES256 private key (AUTH_SIGNING_PRIVATE_KEY_PATH)")
	jwksPath := flags.String("jwks", environmentOr("AUTH_JWKS_PATH", "services/identity-service/.local-keys/jwks.json"), "JWKS holding the key id (AUTH_JWKS_PATH)")
	issuer := flags.String("issuer", environmentOr("AUTH_ISSUER", "http://localhost:3001"), "token issuer (AUTH_ISSUER)")
	audience := flags.String("audience", environmentOr("AUTH_AUDIENCE", "auction-api"), "token audience (AUTH_AUDIENCE)")
	ttl := flags.Duration("ttl", 5*time.Minute, "token lifetime; at most auction-service's ACCESS_TOKEN_TTL_SECONDS")
	err := flags.Parse(args)
	if err != nil {
		return 2
	}
	if *user == "" {
		fmt.Fprintln(os.Stderr, "token: -user is required")
		return 2
	}

	minter, err := token.Load(*keyPath, *jwksPath, *issuer, *audience)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load signing key (run `pnpm auth:keys:local` first): %v\n", err)
		return 1
	}
	value, err := minter.Mint(*user, time.Now(), *ttl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mint token: %v\n", err)
		return 1
	}
	fmt.Println(value)
	return 0
}
