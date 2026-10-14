// Package testinfra holds what the integration tests share: the check that the
// infrastructure variables are set and throwaway databases built from the
// committed schema. Only test files import it.
package testinfra

import (
	"fmt"
	"os"
	"strings"
)

// RequireEnvironment exits the test binary when a variable is missing. Like the
// other test tiers, integration tests never skip themselves: a green run means
// every test ran against real infrastructure. Call it from TestMain.
func RequireEnvironment(names ...string) {
	var missing []string
	for _, name := range names {
		value := os.Getenv(name)
		if value == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return
	}
	fmt.Fprintf(os.Stderr, "integration tests need %s; use `pnpm loadgen:test:integration:local` against the Compose stack\n", strings.Join(missing, ", "))
	os.Exit(1)
}
