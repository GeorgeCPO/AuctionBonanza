package testinfra

import (
	"crypto/rand"
	"encoding/hex"
	"testing"
)

// uniqueName returns prefix plus a random suffix, so concurrent test runs on one
// stack never share a database or topic.
func uniqueName(t testing.TB, prefix string) string {
	t.Helper()
	suffix := make([]byte, 6)
	_, err := rand.Read(suffix)
	if err != nil {
		t.Fatal(err)
	}
	return prefix + hex.EncodeToString(suffix)
}
