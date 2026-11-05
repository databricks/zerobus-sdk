package zerobus_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/auth"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

// newTokenEndpoint serves /oidc/v1/token, replying with status and, on 200,
// a fresh "tok-<n>" access token plus expiresIn (omitted when 0).
func newTokenEndpoint(t *testing.T, status, expiresIn int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"test_error"}`))
			return
		}
		resp := map[string]any{"access_token": fmt.Sprintf("tok-%d", n)}
		if expiresIn > 0 {
			resp["expires_in"] = expiresIn
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestGetTokenSendsZerobusScopedRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/oidc/v1/token" {
			t.Errorf("request = %s %s, want POST /oidc/v1/token", r.Method, r.URL.Path)
		}
		if id, secret, ok := r.BasicAuth(); !ok || id != "client-id" || secret != "client-secret" {
			t.Errorf("basic auth = (%q, %q, %v), want (client-id, client-secret, true)", id, secret, ok)
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm: %v", err)
		}
		want := map[string]string{
			"grant_type": "client_credentials",
			"scope":      "all-apis",
			"resource":   "api://databricks/workspaces/12345/zerobusDirectWriteApi",
		}
		for k, v := range want {
			if got := r.PostForm.Get(k); got != v {
				t.Errorf("%s = %q, want %q", k, got, v)
			}
		}
		if details := r.PostForm.Get("authorization_details"); !strings.Contains(details, `"object_full_path":"cat.sch.tbl"`) {
			t.Errorf("authorization_details not scoped to the table: %s", details)
		}
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer srv.Close()

	tok, err := zerobus.GetToken(context.Background(), srv.URL, "cat.sch.tbl", "client-id", "client-secret", "12345")
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}
	if tok.AccessToken != "tok" {
		t.Fatalf("AccessToken = %q, want %q", tok.AccessToken, "tok")
	}
}

func TestGetTokenExpiryAnchoredToRequestSend(t *testing.T) {
	srv, _ := newTokenEndpoint(t, http.StatusOK, 3600)

	before := time.Now()
	tok, err := zerobus.GetToken(context.Background(), srv.URL, "cat.sch.tbl", "id", "secret", "12345")
	after := time.Now()
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}
	if lo, hi := before.Add(time.Hour), after.Add(time.Hour); tok.ExpiresAt.Before(lo) || tok.ExpiresAt.After(hi) {
		t.Fatalf("ExpiresAt = %v, want within [%v, %v]", tok.ExpiresAt, lo, hi)
	}
}

func TestGetTokenWithoutExpiresInHasZeroExpiry(t *testing.T) {
	srv, _ := newTokenEndpoint(t, http.StatusOK, 0)

	tok, err := zerobus.GetToken(context.Background(), srv.URL, "cat.sch.tbl", "id", "secret", "12345")
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}
	if !tok.ExpiresAt.IsZero() {
		t.Fatalf("ExpiresAt = %v, want zero", tok.ExpiresAt)
	}
}

func TestGetTokenMintsOnEveryCall(t *testing.T) {
	srv, calls := newTokenEndpoint(t, http.StatusOK, 3600)

	first, err := zerobus.GetToken(context.Background(), srv.URL, "cat.sch.tbl", "id", "secret", "12345")
	if err != nil {
		t.Fatalf("first GetToken: %v", err)
	}
	second, err := zerobus.GetToken(context.Background(), srv.URL, "cat.sch.tbl", "id", "secret", "12345")
	if err != nil {
		t.Fatalf("second GetToken: %v", err)
	}
	if calls.Load() != 2 || first.AccessToken == second.AccessToken {
		t.Fatalf("calls = %d, tokens = %q, %q; want 2 distinct mints", calls.Load(), first.AccessToken, second.AccessToken)
	}
}

func TestGetTokenHTTPErrorRetryability(t *testing.T) {
	cases := []struct {
		status    int
		retryable bool
	}{
		{http.StatusUnauthorized, false},
		{http.StatusServiceUnavailable, true},
	}
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			srv, _ := newTokenEndpoint(t, tc.status, 0)

			_, err := zerobus.GetToken(context.Background(), srv.URL, "cat.sch.tbl", "id", "secret", "12345")
			var zerr *zerobus.Error
			if !errors.As(err, &zerr) || zerr.Op != "GetToken" {
				t.Fatalf("err = %v, want *zerobus.Error with Op GetToken", err)
			}
			if zerr.Retryable() != tc.retryable || zerobus.Retryable(err) != tc.retryable {
				t.Fatalf("retryable = %v, want %v", zerr.Retryable(), tc.retryable)
			}
			var tokenErr *auth.TokenError
			if !errors.As(err, &tokenErr) {
				t.Fatalf("errors.As(*auth.TokenError) failed for %v", err)
			}
		})
	}
}

func TestGetTokenRejectsInvalidInputWithoutRequest(t *testing.T) {
	srv, calls := newTokenEndpoint(t, http.StatusOK, 3600)

	cases := []struct {
		name, url, table, clientID, secret, wsID, wantErr string
	}{
		{"plaintext http non-loopback URL", "http://example.com", "cat.sch.tbl", "id", "secret", "12345", "must use https"},
		{"empty URL", "", "cat.sch.tbl", "id", "secret", "12345", "ucEndpoint is required"},
		{"two-part table name", srv.URL, "cat.sch", "id", "secret", "12345", "table name must be catalog.schema.table"},
		{"empty schema", srv.URL, "cat..tbl", "id", "secret", "12345", "schema part of table name is empty"},
		{"empty client ID", srv.URL, "cat.sch.tbl", "", "secret", "12345", "clientID is required"},
		{"empty client secret", srv.URL, "cat.sch.tbl", "id", "", "12345", "clientSecret is required"},
		{"empty workspace ID", srv.URL, "cat.sch.tbl", "id", "secret", " ", "workspaceID is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := zerobus.GetToken(context.Background(), tc.url, tc.table, tc.clientID, tc.secret, tc.wsID)
			var zerr *zerobus.Error
			if !errors.As(err, &zerr) || zerr.Op != "GetToken" || zerr.Retryable() {
				t.Fatalf("err = %v, want non-retryable *zerobus.Error with Op GetToken", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("token endpoint called %d times, want 0", n)
	}
}

func TestOAuthTokenFormattingRedactsAccessToken(t *testing.T) {
	expiresAt := time.Date(2026, time.November, 5, 15, 45, 0, 0, time.UTC)
	cases := []struct {
		tok        zerobus.OAuthToken
		wantExpiry string
	}{
		{zerobus.OAuthToken{AccessToken: "secret-token-value", ExpiresAt: expiresAt}, "2026-11-05T15:45:00Z"},
		{zerobus.OAuthToken{AccessToken: "secret-token-value"}, "none"},
	}
	for _, tc := range cases {
		for _, verb := range []string{"%v", "%s", "%+v"} {
			for _, arg := range []any{tc.tok, &tc.tok} {
				got := fmt.Sprintf(verb, arg)
				if strings.Contains(got, "secret-token-value") || !strings.Contains(got, "<redacted>") || !strings.Contains(got, tc.wantExpiry) {
					t.Errorf("Sprintf(%q) = %q, want redacted token and expiry %q", verb, got, tc.wantExpiry)
				}
			}
		}
		if got := fmt.Sprintf("%#v", tc.tok); strings.Contains(got, "secret-token-value") || !strings.Contains(got, `AccessToken:"<redacted>"`) {
			t.Errorf(`Sprintf("%%#v") = %q, want redacted token`, got)
		}
	}
}
