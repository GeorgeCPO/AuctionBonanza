// Command loadgen seeds an Auction Bonanza stack with users and auctions and
// samples PostgreSQL and Kafka while it runs.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

const usage = `usage: loadgen <command> [flags]

commands:
  seed     create users and auctions and write a manifest
  token    print an access token for a user id, for trying the API by hand
  sample   poll PostgreSQL and Kafka and write CSV (added in a later commit)

Run "loadgen <command> -h" for a command's flags. Paths default to running
from the repository root.`

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case "seed":
		return runSeed(ctx, args[1:])
	case "token":
		return runToken(args[1:])
	case "-h", "--help", "help":
		fmt.Println(usage)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s\n", args[0], usage)
		return 2
	}
}

func environmentOr(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}
