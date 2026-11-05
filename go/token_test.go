package zerobus

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testTable       = "main.default.events"
	testWorkspaceID = "1234567890"
	testToken       = "secret-token-value"
)

func tokenServer(t *testing.T, status int, body string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/oidc/v1/token" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		if !strings.Contains(r.PostForm.Get("resource"), testWorkspaceID) {
			t.Errorf("resource %q does not name the workspace", r.PostForm.Get("resource"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)
	return server, &requests
}

func fetchTestToken(url, table string) (OAuthToken, error) {
	return GetToken(url, table, "client-id", "client-secret", testWorkspaceID)
}

func TestGetTokenReturnsTokenAndExpiry(t *testing.T) {
	server, _ := tokenServer(t, http.StatusOK, `{"access_token":"`+testToken+`","expires_in":3600}`)

	before := time.Now().Truncate(time.Millisecond)
	token, err := fetchTestToken(server.URL, testTable)
	after := time.Now()
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}

	if token.AccessToken != testToken {
		t.Errorf("AccessToken = %q, want %q", token.AccessToken, testToken)
	}
	if token.ExpiresAt.Before(before.Add(time.Hour)) || token.ExpiresAt.After(after.Add(time.Hour)) {
		t.Errorf("ExpiresAt = %v, want within an hour of the request", token.ExpiresAt)
	}
}

func TestGetTokenMissingExpiresInYieldsZeroExpiry(t *testing.T) {
	server, _ := tokenServer(t, http.StatusOK, `{"access_token":"`+testToken+`"}`)

	token, err := fetchTestToken(server.URL, testTable)
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}
	if !token.ExpiresAt.IsZero() {
		t.Errorf("ExpiresAt = %v, want zero", token.ExpiresAt)
	}
}

func TestGetTokenMintsOnEveryCall(t *testing.T) {
	server, requests := tokenServer(t, http.StatusOK, `{"access_token":"`+testToken+`","expires_in":3600}`)

	for i := 0; i < 2; i++ {
		if _, err := fetchTestToken(server.URL, testTable); err != nil {
			t.Fatalf("GetToken: %v", err)
		}
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("token requests = %d, want 2", got)
	}
}

func TestGetTokenClassifiesErrors(t *testing.T) {
	tests := []struct {
		status    int
		retryable bool
	}{
		{http.StatusUnauthorized, false},
		{http.StatusServiceUnavailable, true},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			server, _ := tokenServer(t, tt.status, `{"error":"failed"}`)

			_, err := fetchTestToken(server.URL, testTable)
			var zerr *ZerobusError
			if !errors.As(err, &zerr) {
				t.Fatalf("err = %v, want *ZerobusError", err)
			}
			if zerr.IsRetryable != tt.retryable {
				t.Errorf("IsRetryable = %v, want %v", zerr.IsRetryable, tt.retryable)
			}
		})
	}
}

func TestGetTokenInvalidTableNameFailsWithoutRequest(t *testing.T) {
	server, requests := tokenServer(t, http.StatusOK, `{"access_token":"`+testToken+`"}`)

	_, err := fetchTestToken(server.URL, "not-a-table")
	var zerr *ZerobusError
	if !errors.As(err, &zerr) || zerr.IsRetryable {
		t.Fatalf("err = %v, want non-retryable *ZerobusError", err)
	}
	if got := requests.Load(); got != 0 {
		t.Errorf("token requests = %d, want 0", got)
	}
}

func TestOAuthTokenFormattingRedactsAccessToken(t *testing.T) {
	token := OAuthToken{AccessToken: testToken, ExpiresAt: time.Now()}
	for _, verb := range []string{"%v", "%+v", "%s", "%#v"} {
		if got := fmt.Sprintf(verb, token); strings.Contains(got, testToken) {
			t.Errorf("%s leaks the token: %s", verb, got)
		}
	}
}
