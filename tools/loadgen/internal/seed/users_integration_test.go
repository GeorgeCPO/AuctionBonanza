//go:build integration

package seed

import (
	"context"
	"os"
	"testing"

	"github.com/GeorgeCPO/AuctionBonanza/tools/loadgen/internal/testinfra"
)

func TestMain(m *testing.M) {
	testinfra.RequireEnvironment("TEST_DATABASE_URL")
	os.Exit(m.Run())
}

func TestEnsureUsersIsIdempotentAndGrows(t *testing.T) {
	ctx := context.Background()
	pool := testinfra.NewDatabase(t, os.Getenv("TEST_DATABASE_URL"))
	store := NewPostgresUserStore(pool)

	first, err := store.EnsureUsers(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.EnsureUsers(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	grown, err := store.EnsureUsers(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}

	if len(first) != 5 || len(grown) != 8 {
		t.Fatalf("got %d, then %d users, want 5 and 8", len(first), len(grown))
	}
	seen := map[string]bool{}
	for i, user := range first {
		if again[i] != user || grown[i] != user {
			t.Errorf("user %d changed between runs: %+v, %+v, %+v", i, user, again[i], grown[i])
		}
		if seen[user.ID] {
			t.Errorf("duplicate id %s", user.ID)
		}
		seen[user.ID] = true
	}

	var count int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&count)
	if err != nil || count != 8 {
		t.Fatalf("users table holds %d rows (err %v), want 8", count, err)
	}
}

func TestEnsureUsersReusesAnAccountWithDifferentEmailCase(t *testing.T) {
	ctx := context.Background()
	pool := testinfra.NewDatabase(t, os.Getenv("TEST_DATABASE_URL"))
	_, err := pool.Exec(ctx,
		`INSERT INTO users (username, password_hash, email) VALUES ('someone', 'x', 'LoadGen-2@loadgen.invalid')`)
	if err != nil {
		t.Fatal(err)
	}

	users, err := NewPostgresUserStore(pool).EnsureUsers(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}

	var count int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&count)
	if err != nil || count != 3 {
		t.Fatalf("users table holds %d rows (err %v), want the existing account reused", count, err)
	}
	var existingID string
	err = pool.QueryRow(ctx, `SELECT id::text FROM users WHERE username = 'someone'`).Scan(&existingID)
	if err != nil || users[1].ID != existingID {
		t.Fatalf("user 2 is %s, want the pre-existing account %s (err %v)", users[1].ID, existingID, err)
	}
}

func TestSeededUsersCannotLogIn(t *testing.T) {
	ctx := context.Background()
	pool := testinfra.NewDatabase(t, os.Getenv("TEST_DATABASE_URL"))
	_, err := NewPostgresUserStore(pool).EnsureUsers(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}

	var matching int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE password_hash LIKE '$argon2id$%'`).Scan(&matching)
	if err != nil || matching != 0 {
		t.Fatalf("%d seeded users carry a verifiable argon2id hash (err %v)", matching, err)
	}
}
