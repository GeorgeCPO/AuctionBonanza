package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/token"
)

func environmentOr(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

func databaseURLFlag(flags *flag.FlagSet) *string {
	return flags.String("database-url", environmentOr("DATABASE_URL", "postgresql://auction:auction_local@localhost:5432/auction_db"), "PostgreSQL URL (DATABASE_URL)")
}

func brokersFlag(flags *flag.FlagSet) *string {
	return flags.String("brokers", environmentOr("KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka brokers (KAFKA_BROKERS)")
}

func topicFlag(flags *flag.FlagSet) *string {
	return flags.String("topic", environmentOr("AUCTION_EVENTS_TOPIC", "auction.events"), "topic of auction events (AUCTION_EVENTS_TOPIC)")
}

// authFlags are the flags that say how to sign access tokens, shared by every
// command that acts as a user.
type authFlags struct {
	keyPath, jwksPath, issuer, audience *string
	ttl                                 *time.Duration
}

func registerAuthFlags(flags *flag.FlagSet) authFlags {
	return authFlags{
		keyPath:  flags.String("key", environmentOr("AUTH_SIGNING_PRIVATE_KEY_PATH", "services/identity-service/.local-keys/private-key.pem"), "ES256 private key (AUTH_SIGNING_PRIVATE_KEY_PATH)"),
		jwksPath: flags.String("jwks", environmentOr("AUTH_JWKS_PATH", "services/identity-service/.local-keys/jwks.json"), "JWKS holding the key id (AUTH_JWKS_PATH)"),
		issuer:   flags.String("issuer", environmentOr("AUTH_ISSUER", "http://localhost:3001"), "token issuer (AUTH_ISSUER)"),
		audience: flags.String("audience", environmentOr("AUTH_AUDIENCE", "auction-api"), "token audience (AUTH_AUDIENCE)"),
		ttl:      flags.Duration("ttl", token.DefaultTTL, "token lifetime; at most auction-service's ACCESS_TOKEN_TTL_SECONDS"),
	}
}

func (a authFlags) minter() (*token.Minter, error) {
	minter, err := token.Load(*a.keyPath, *a.jwksPath, *a.issuer, *a.audience, *a.ttl)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w (run `pnpm auth:keys:local` first)", err)
	}
	return minter, err
}

func splitList(value string) []string {
	var items []string
	for _, item := range strings.Split(value, ",") {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}
