package token

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func decodeSegment(t *testing.T, segment string, into any) {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		t.Fatal(err)
	}
	err = json.Unmarshal(raw, into)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMintProducesATokenAuctionServiceCanVerify(t *testing.T) {
	key := newTestKey(t)
	minter, err := New(key, "local-test", "http://localhost:3001", "auction-api", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)

	value, err := minter.Mint("42", now)
	if err != nil {
		t.Fatal(err)
	}

	segments := strings.Split(value, ".")
	if len(segments) != 3 {
		t.Fatalf("token has %d segments, want 3", len(segments))
	}

	var h header
	decodeSegment(t, segments[0], &h)
	if h.Algorithm != "ES256" || h.Type != "at+jwt" || h.KeyID != "local-test" {
		t.Errorf("unexpected header %+v", h)
	}

	var c Claims
	decodeSegment(t, segments[1], &c)
	if c.Issuer != "http://localhost:3001" || c.Audience != "auction-api" || c.Subject != "42" {
		t.Errorf("unexpected identity claims %+v", c)
	}
	if c.SessionID == "" || c.TokenID == "" || c.SessionID == c.TokenID {
		t.Errorf("session id %q and token id %q must be set and distinct", c.SessionID, c.TokenID)
	}
	if c.IssuedAt != now.Unix() || c.ExpiresAt-c.IssuedAt != 600 {
		t.Errorf("iat %d exp %d, want a 600 second lifetime from %d", c.IssuedAt, c.ExpiresAt, now.Unix())
	}

	raw, err := base64.RawURLEncoding.DecodeString(segments[2])
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 64 {
		t.Fatalf("signature is %d bytes, JWS ES256 needs 64", len(raw))
	}
	digest := sha256.Sum256([]byte(segments[0] + "." + segments[1]))
	r := new(big.Int).SetBytes(raw[:32])
	s := new(big.Int).SetBytes(raw[32:])
	if !ecdsa.Verify(&key.PublicKey, digest[:], r, s) {
		t.Fatal("signature does not verify against the public key")
	}
}

func TestMintGivesEveryTokenItsOwnIDs(t *testing.T) {
	minter, err := New(newTestKey(t), "k", "iss", "aud", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()

	first, _ := minter.Mint("1", now)
	second, _ := minter.Mint("1", now)
	if first == second {
		t.Fatal("two tokens for the same user were identical")
	}
}

func TestNewRejectsWrongCurveAndMissingFields(t *testing.T) {
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(p384, "k", "iss", "aud", time.Minute)
	if err == nil {
		t.Error("P-384 key was accepted")
	}

	_, err = New(newTestKey(t), "", "iss", "aud", time.Minute)
	if err == nil {
		t.Error("empty key id was accepted")
	}

	for _, ttl := range []time.Duration{0, -time.Second} {
		_, err = New(newTestKey(t), "k", "iss", "aud", ttl)
		if err == nil {
			t.Errorf("token lifetime %s was accepted; it would mint tokens that are already expired", ttl)
		}
	}
}

func TestLoadReadsTheLocalKeyMaterial(t *testing.T) {
	key := newTestKey(t)
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	keyPath := filepath.Join(directory, "private-key.pem")
	jwksPath := filepath.Join(directory, "jwks.json")
	err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(jwksPath, []byte(`{"keys":[{"kid":"local-2026-09","alg":"ES256","use":"sig"}]}`), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	minter, err := Load(keyPath, jwksPath, "http://localhost:3001", "auction-api", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	value, err := minter.Mint("7", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	var h header
	decodeSegment(t, strings.Split(value, ".")[0], &h)
	if h.KeyID != "local-2026-09" {
		t.Errorf("kid %q, want local-2026-09", h.KeyID)
	}
}

func TestLoadReportsMissingFiles(t *testing.T) {
	_, err := Load("/nonexistent/key.pem", "/nonexistent/jwks.json", "iss", "aud", time.Minute)
	if err == nil {
		t.Fatal("expected an error for a missing key file")
	}
}
