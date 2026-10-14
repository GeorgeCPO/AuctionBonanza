// Package token mints the ES256 access tokens that auction-service accepts, so
// the load tool can act as any seeded user without calling identity-service.
// The claims mirror what packages/access-auth verifies.
package token

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"time"
)

const (
	tokenType      = "at+jwt"
	coordinateSize = 32
	// DefaultTTL is a token lifetime that fits auction-service's default
	// ACCESS_TOKEN_TTL_SECONDS (600), which rejects any longer token.
	DefaultTTL = 5 * time.Minute
)

// Minter signs access tokens with one private key. It holds no mutable state,
// so it is safe for concurrent use.
type Minter struct {
	key      *ecdsa.PrivateKey
	keyID    string
	issuer   string
	audience string
	ttl      time.Duration
}

// Claims is the payload of an access token.
type Claims struct {
	Issuer    string `json:"iss"`
	Audience  string `json:"aud"`
	Subject   string `json:"sub"`
	SessionID string `json:"sid"`
	TokenID   string `json:"jti"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

type header struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
	KeyID     string `json:"kid"`
}

type jwks struct {
	Keys []struct {
		KeyID string `json:"kid"`
	} `json:"keys"`
}

// New builds a Minter from an ECDSA P-256 key. Every token it mints lives for
// ttl, which auction-service only accepts up to its ACCESS_TOKEN_TTL_SECONDS.
func New(key *ecdsa.PrivateKey, keyID, issuer, audience string, ttl time.Duration) (*Minter, error) {
	if key.Curve != elliptic.P256() {
		return nil, errors.New("signing key must be on curve P-256")
	}
	if keyID == "" || issuer == "" || audience == "" {
		return nil, errors.New("key id, issuer and audience are required")
	}
	if ttl <= 0 {
		return nil, errors.New("token lifetime must be positive")
	}
	return &Minter{key: key, keyID: keyID, issuer: issuer, audience: audience, ttl: ttl}, nil
}

// Load reads the local signing material that `pnpm auth:keys:local` writes: a
// PKCS#8 PEM private key and the JWKS holding the key id identity publishes.
func Load(privateKeyPath, jwksPath, issuer, audience string, ttl time.Duration) (*Minter, error) {
	pemBytes, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read signing key: %w", err)
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("signing key %s is not PEM", privateKeyPath)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse signing key: %w", err)
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("signing key %s is not an ECDSA key", privateKeyPath)
	}

	jwksBytes, err := os.ReadFile(jwksPath)
	if err != nil {
		return nil, fmt.Errorf("read jwks: %w", err)
	}
	var set jwks
	err = json.Unmarshal(jwksBytes, &set)
	if err != nil {
		return nil, fmt.Errorf("parse jwks: %w", err)
	}
	if len(set.Keys) == 0 {
		return nil, fmt.Errorf("jwks %s has no keys", jwksPath)
	}

	return New(key, set.Keys[0].KeyID, issuer, audience, ttl)
}

// Mint returns a signed access token for a user, valid from now for the
// Minter's lifetime.
func (m *Minter) Mint(userID string, now time.Time) (string, error) {
	sessionID, err := randomID()
	if err != nil {
		return "", err
	}
	tokenID, err := randomID()
	if err != nil {
		return "", err
	}

	claims := Claims{
		Issuer:    m.issuer,
		Audience:  m.audience,
		Subject:   userID,
		SessionID: sessionID,
		TokenID:   tokenID,
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(m.ttl).Unix(),
	}
	signingInput, err := encodeSigningInput(header{Algorithm: "ES256", Type: tokenType, KeyID: m.keyID}, claims)
	if err != nil {
		return "", err
	}

	// JWS wants the signature as r||s, each 32 bytes, which is what Sign returns
	// before ASN.1 encoding would hide it.
	digest := sha256.Sum256([]byte(signingInput))
	r, s, err := ecdsa.Sign(rand.Reader, m.key, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign token: %w", err)
	}
	raw := make([]byte, 2*coordinateSize)
	r.FillBytes(raw[:coordinateSize])
	s.FillBytes(raw[coordinateSize:])

	return signingInput + "." + base64.RawURLEncoding.EncodeToString(raw), nil
}

func encodeSigningInput(h header, c Claims) (string, error) {
	headerJSON, err := json.Marshal(h)
	if err != nil {
		return "", err
	}
	claimsJSON, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(headerJSON) + "." +
		base64.RawURLEncoding.EncodeToString(claimsJSON), nil
}

func randomID() (string, error) {
	bytes := make([]byte, 16)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", fmt.Errorf("random id: %w", err)
	}
	// RFC 4122 version 4 layout.
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(bytes)
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}
