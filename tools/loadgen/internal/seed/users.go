package seed

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	userEmailFormat      = "loadgen-%d@loadgen.invalid"
	userNameFormat       = "loadgen-%d"
	unusablePasswordHash = "!loadgen-unusable"
)

// User is a seeded account.
type User struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

// PostgresUserStore inserts users straight into the users table. Identity owns
// that table and hashes passwords with argon2id on purpose, which makes
// registering thousands of users over HTTP slow. Seeded users get a hash no
// password can match, so they can only be used through minted tokens.
type PostgresUserStore struct {
	pool *pgxpool.Pool
}

func NewPostgresUserStore(pool *pgxpool.Pool) *PostgresUserStore {
	return &PostgresUserStore{pool: pool}
}

// EnsureUsers makes sure users loadgen-1 through loadgen-count exist and
// returns them in that order. Existing users are reused, so a rerun adds
// nothing.
func (s *PostgresUserStore) EnsureUsers(ctx context.Context, count int) ([]User, error) {
	emails := make([]string, count)
	names := make([]string, count)
	for i := range emails {
		emails[i] = fmt.Sprintf(userEmailFormat, i+1)
		names[i] = fmt.Sprintf(userNameFormat, i+1)
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO users (username, password_hash, email)
		SELECT name, $3, email FROM unnest($1::text[], $2::text[]) AS seeded(name, email)
		ON CONFLICT DO NOTHING`, names, emails, unusablePasswordHash)
	if err != nil {
		return nil, fmt.Errorf("insert users: %w", err)
	}

	rows, err := s.pool.Query(ctx,
		`SELECT id::text, lower(email) FROM users WHERE lower(email) = ANY($1::text[])`, emails)
	if err != nil {
		return nil, fmt.Errorf("read users back: %w", err)
	}
	defer rows.Close()

	idByEmail := make(map[string]string, count)
	for rows.Next() {
		var id, email string
		err = rows.Scan(&id, &email)
		if err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		idByEmail[email] = id
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("read users back: %w", err)
	}

	users := make([]User, count)
	for i, email := range emails {
		id, found := idByEmail[email]
		if !found {
			return nil, fmt.Errorf("user %s missing after insert", email)
		}
		users[i] = User{ID: id, Email: email}
	}
	return users, nil
}
