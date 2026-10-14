package seed

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientCreatesAnAuctionWithABearerToken(t *testing.T) {
	var gotAuthorization string
	var gotBody CreateRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthorization = r.Header.Get("Authorization")
		if r.Method != http.MethodPost || r.URL.Path != "/v1/auctions" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"77","ownerId":"3","currencyCode":"USD","endsAt":"2030-01-01T00:00:00.000Z","startingPriceMinor":"10000","minimumBidIncrementMinor":"100","phase":"open"}`))
	}))
	defer server.Close()

	client := NewAuctionClient(server.URL+"/", server.Client())
	created, err := client.Create(context.Background(), "token-1", CreateRequest{Title: "Lot", StartingPriceMinor: "10000"})
	if err != nil {
		t.Fatal(err)
	}

	if created.ID != "77" || created.OwnerID != "3" {
		t.Errorf("unexpected created auction %+v", created)
	}
	if gotAuthorization != "Bearer token-1" {
		t.Errorf("authorization %q", gotAuthorization)
	}
	if gotBody.Title != "Lot" {
		t.Errorf("body not forwarded: %+v", gotBody)
	}
}

func TestClientReportsNon201AsAStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":"INVALID_AUCTION"}`))
	}))
	defer server.Close()

	client := NewAuctionClient(server.URL, server.Client())
	_, err := client.Create(context.Background(), "t", CreateRequest{})

	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected a 400 StatusError, got %v", err)
	}
	if errors.Is(err, ErrNotSent) {
		t.Error("a response from the server must not look like a send failure")
	}
}

func TestClientMarksARefusedConnectionAsNotSent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	address := server.URL
	server.Close()

	client := NewAuctionClient(address, http.DefaultClient)
	_, err := client.Create(context.Background(), "t", CreateRequest{})

	if !errors.Is(err, ErrNotSent) {
		t.Fatalf("expected ErrNotSent for a refused connection, got %v", err)
	}
}

func TestClientDoesNotMarkALostResponseAsNotSent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("response writer cannot hijack")
			return
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		connection.Close()
	}))
	defer server.Close()

	client := NewAuctionClient(server.URL, server.Client())
	_, err := client.Create(context.Background(), "t", CreateRequest{})

	if err == nil {
		t.Fatal("expected an error when the server drops the connection")
	}
	if errors.Is(err, ErrNotSent) {
		t.Error("the request reached the server, so the outcome is unknown, not 'not sent'")
	}
}

func TestClientRejectsAResponseWithoutAnID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := NewAuctionClient(server.URL, server.Client())
	_, err := client.Create(context.Background(), "t", CreateRequest{})
	if err == nil {
		t.Fatal("expected an error for a response with no id")
	}
}
