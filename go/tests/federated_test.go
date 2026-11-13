package tests

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	zerobus "github.com/databricks/zerobus-sdk/go"
)

// Federation tests: each runs real streams against the mock gRPC server, with the
// SDK's Unity Catalog URL pointed at a mock token endpoint that records every
// exchange request.

const (
	fedTable  = "test_catalog.test_schema.fed_table"
	fedTable2 = "test_catalog.test_schema.fed_table_2"
	spID      = "11111111-2222-3333-4444-555555555555"
)

// fakeJWT returns a realistic ~1.5 KB JWT-shaped token that expires in 2 h, so
// the subject expiry never caps the exchanged token's lifetime.
func fakeJWT(t *testing.T, subject string) string {
	t.Helper()
	enc := base64.RawURLEncoding
	pad := make([]byte, 700)
	sig := make([]byte, 256)
	if _, err := rand.Read(pad); err != nil {
		t.Fatal(err)
	}
	_, _ = rand.Read(sig)
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	payload, _ := json.Marshal(map[string]any{
		"sub": subject, "aud": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
		"exp": time.Now().Add(2 * time.Hour).Unix(), "pad": enc.EncodeToString(pad),
	})
	return enc.EncodeToString(header) + "." + enc.EncodeToString(payload) + "." + enc.EncodeToString(sig)
}

// countingSupplier returns the scripted tokens in order (the last one repeats)
// and counts calls.
type countingSupplier struct {
	calls  atomic.Int32
	tokens []string
	delay  time.Duration
}

func (c *countingSupplier) get() (string, error) {
	n := int(c.calls.Add(1)) - 1
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	if n >= len(c.tokens) {
		n = len(c.tokens) - 1
	}
	return c.tokens[n], nil
}

// fedEnv is one SDK wired to a mock gRPC server and a mock token endpoint.
type fedEnv struct {
	sdk   *zerobus.ZerobusSdk
	grpc  *MockZerobusServer
	token *MockTokenServer
}

func newFedEnv(t *testing.T, script ...TokenReply) *fedEnv {
	t.Helper()
	grpcMock, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(grpcServer.Stop)
	for _, table := range []string{fedTable, fedTable2} {
		responses := make([]MockResponse, 32)
		for i := range responses {
			responses[i] = CreateStreamResponse(fmt.Sprintf("s-%d", i), 0)
		}
		grpcMock.InjectResponses(table, responses)
	}
	tokenMock := NewMockTokenServer(script...)
	t.Cleanup(tokenMock.Close)

	sdk, err := zerobus.NewZerobusSdk(serverURL, tokenMock.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sdk.Free)
	return &fedEnv{sdk: sdk, grpc: grpcMock, token: tokenMock}
}

func jsonOpts() *zerobus.StreamConfigurationOptions {
	opts := zerobus.DefaultStreamConfigurationOptions()
	opts.Recovery = false
	opts.RecordType = zerobus.RecordTypeJson
	return opts
}

func newToken(t *testing.T, supplier zerobus.IdpTokenSupplier, opts ...zerobus.FederatedTokenOption) *zerobus.FederatedToken {
	t.Helper()
	token, err := zerobus.NewFederatedToken(supplier, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// open creates a JSON stream on table and closes it at the end of the test.
func (e *fedEnv) open(t *testing.T, table string, token *zerobus.FederatedToken) error {
	t.Helper()
	stream, err := e.sdk.CreateStreamWithFederatedToken(zerobus.TableProperties{TableName: table}, token, jsonOpts())
	if err == nil {
		t.Cleanup(func() { stream.Close() })
	}
	return err
}

func (e *fedEnv) mustOpen(t *testing.T, table string, token *zerobus.FederatedToken) {
	t.Helper()
	if err := e.open(t, table, token); err != nil {
		t.Fatalf("create federated stream: %v", err)
	}
}

func asZerobusError(t *testing.T, err error) *zerobus.ZerobusError {
	t.Helper()
	var zerr *zerobus.ZerobusError
	if !errors.As(err, &zerr) {
		t.Fatalf("expected *ZerobusError, got %T: %v", err, err)
	}
	return zerr
}

// The exchange request on the wire, in both modes, and the minted
// token reaching the gRPC stream.
func TestFederatedExchangeWireContract(t *testing.T) {
	for _, mode := range []string{"account_level", "workload_identity"} {
		t.Run(mode, func(t *testing.T) {
			env := newFedEnv(t)
			jwt := fakeJWT(t, "sub-"+mode)
			var opts []zerobus.FederatedTokenOption
			if mode == "workload_identity" {
				opts = append(opts, zerobus.WithClientID(spID))
			}
			env.mustOpen(t, fedTable, newToken(t, (&countingSupplier{tokens: []string{jwt}}).get, opts...))

			reqs := env.token.Requests()
			if len(reqs) != 1 {
				t.Fatalf("exchanges = %d, want 1", len(reqs))
			}
			r := reqs[0]
			// Path, method, content type, and no client-credentials Authorization.
			if r.Method != "POST" || r.Path != "/oidc/v1/token" {
				t.Errorf("request = %s %s, want POST /oidc/v1/token", r.Method, r.Path)
			}
			if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/x-www-form-urlencoded") {
				t.Errorf("Content-Type = %q", ct)
			}
			if auth := r.Header.Get("Authorization"); auth != "" {
				t.Errorf("exchange carried Authorization %q, want none", auth)
			}
			// The supplier's token, byte for byte.
			if got := r.Form.Get("subject_token"); got != jwt {
				t.Errorf("subject_token differs from the supplier's token (len %d vs %d)", len(got), len(jwt))
			}
			// The token-exchange grant.
			if g := r.Form.Get("grant_type"); g != "urn:ietf:params:oauth:grant-type:token-exchange" {
				t.Errorf("grant_type = %q", g)
			}
			if st := r.Form.Get("subject_token_type"); st != "urn:ietf:params:oauth:token-type:jwt" {
				t.Errorf("subject_token_type = %q", st)
			}
			// client_id only for workload identity federation.
			if mode == "workload_identity" {
				if got := r.Form["client_id"]; len(got) != 1 || got[0] != spID {
					t.Errorf("client_id = %v, want [%s]", got, spID)
				}
			} else if _, ok := r.Form["client_id"]; ok {
				t.Errorf("account-level exchange sent client_id %v", r.Form["client_id"])
			}
			// The minted token authenticates the gRPC stream.
			auths := env.grpc.GetStreamAuths()
			if len(auths) == 0 || auths[0].Authorization != "Bearer dbx-token-0" || auths[0].TableName != fedTable {
				t.Errorf("gRPC stream auth = %+v, want Bearer dbx-token-0 for %s", auths, fedTable)
			}
		})
	}
}

// waitFor polls cond for up to 10 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// One FederatedToken shares its exchanged token across streams.
func TestFederatedTokenSharedAcrossStreams(t *testing.T) {
	env := newFedEnv(t)
	supplier := &countingSupplier{tokens: []string{fakeJWT(t, "a")}}
	token := newToken(t, supplier.get)
	env.mustOpen(t, fedTable, token)
	env.mustOpen(t, fedTable, token)

	if c := supplier.calls.Load(); c != 1 {
		t.Errorf("supplier calls = %d, want 1 (second stream must hit the cache)", c)
	}
	if n := env.token.Count(); n != 1 {
		t.Errorf("exchanges = %d, want 1", n)
	}
}

// Two account-level tokens never share an exchanged token (cross-identity reuse).
func TestFederatedDistinctTokensIsolate(t *testing.T) {
	env := newFedEnv(t)
	jwtA, jwtB := fakeJWT(t, "alice"), fakeJWT(t, "bob")
	env.mustOpen(t, fedTable, newToken(t, (&countingSupplier{tokens: []string{jwtA}}).get))
	env.mustOpen(t, fedTable, newToken(t, (&countingSupplier{tokens: []string{jwtB}}).get))

	reqs := env.token.Requests()
	if len(reqs) != 2 {
		t.Fatalf("exchanges = %d, want 2", len(reqs))
	}
	if reqs[0].Form.Get("subject_token") != jwtA || reqs[1].Form.Get("subject_token") != jwtB {
		t.Error("each exchange must carry its own token's subject_token")
	}
	auths := env.grpc.GetStreamAuths()
	if len(auths) < 2 || auths[1].Authorization != "Bearer dbx-token-1" {
		t.Errorf("second stream auth = %+v, want its own Bearer dbx-token-1", auths)
	}
}

// Workload identity caches by service principal, so the same client_id shares.
func TestFederatedWorkloadIdentityKeyedByClientID(t *testing.T) {
	env := newFedEnv(t)
	jwt := fakeJWT(t, "wif")
	env.mustOpen(t, fedTable, newToken(t, (&countingSupplier{tokens: []string{jwt}}).get, zerobus.WithClientID(spID)))
	env.mustOpen(t, fedTable, newToken(t, (&countingSupplier{tokens: []string{jwt}}).get, zerobus.WithClientID(spID)))
	if n := env.token.Count(); n != 1 {
		t.Errorf("same client_id: exchanges = %d, want 1", n)
	}
	env.mustOpen(t, fedTable, newToken(t, (&countingSupplier{tokens: []string{jwt}}).get,
		zerobus.WithClientID("99999999-8888-7777-6666-555555555555")))
	if n := env.token.Count(); n != 2 {
		t.Errorf("different client_id: exchanges = %d, want 2", n)
	}
}

// Exchanged tokens are table-scoped.
func TestFederatedTokenPerTable(t *testing.T) {
	env := newFedEnv(t)
	token := newToken(t, (&countingSupplier{tokens: []string{fakeJWT(t, "a")}}).get)
	env.mustOpen(t, fedTable, token)
	env.mustOpen(t, fedTable2, token)
	if n := env.token.Count(); n != 2 {
		t.Errorf("exchanges = %d, want 2 (one per table)", n)
	}
}

// A supplier error is retryable, is not cached, and never leaks the token.
func TestFederatedSupplierErrorRetryableAndNotCached(t *testing.T) {
	env := newFedEnv(t)
	jwt := fakeJWT(t, "a")
	var calls atomic.Int32
	token := newToken(t, func() (string, error) {
		if calls.Add(1) == 1 {
			return "", errors.New("idp throttled")
		}
		return jwt, nil
	})

	err := env.open(t, fedTable, token)
	if err == nil {
		t.Fatal("expected the first create to fail")
	}
	zerr := asZerobusError(t, err)
	if !zerr.IsRetryable || !strings.Contains(zerr.Message, "idp throttled") {
		t.Errorf("got %q (retryable=%v), want a retryable error naming the cause", zerr.Message, zerr.IsRetryable)
	}
	if strings.Contains(zerr.Message, jwt) {
		t.Error("error message leaks the IdP token")
	}
	if n := env.token.Count(); n != 0 {
		t.Errorf("exchanges after a supplier error = %d, want 0", n)
	}

	env.mustOpen(t, fedTable, token) // not cached: the supplier runs again
	if c := calls.Load(); c != 2 || env.token.Count() != 1 {
		t.Errorf("supplier calls = %d, exchanges = %d; want 2 and 1", c, env.token.Count())
	}
}

// A non-retryable *ZerobusError stays non-retryable.
func TestFederatedSupplierPermanentError(t *testing.T) {
	env := newFedEnv(t)
	token := newToken(t, func() (string, error) {
		return "", fmt.Errorf("fetch token: %w", &zerobus.ZerobusError{Message: "idp unavailable"})
	})
	zerr := asZerobusError(t, env.open(t, fedTable, token))
	if zerr.IsRetryable || !strings.Contains(zerr.Message, "idp unavailable") {
		t.Errorf("got %q (retryable=%v), want a non-retryable error", zerr.Message, zerr.IsRetryable)
	}
}

// An empty token is rejected as non-retryable, and never sent.
func TestFederatedSupplierEmptyTokenRejected(t *testing.T) {
	env := newFedEnv(t)
	zerr := asZerobusError(t, env.open(t, fedTable, newToken(t, func() (string, error) { return "", nil })))
	if zerr.IsRetryable || !strings.Contains(zerr.Message, "empty token") {
		t.Errorf("got %q (retryable=%v), want a non-retryable empty-token error", zerr.Message, zerr.IsRetryable)
	}
	if n := env.token.Count(); n != 0 {
		t.Errorf("exchanges = %d, want 0 (an empty subject_token must never be sent)", n)
	}
}

// A panicking supplier is contained, and the SDK stays usable.
func TestFederatedSupplierPanicIsContained(t *testing.T) {
	env := newFedEnv(t)
	zerr := asZerobusError(t, env.open(t, fedTable, newToken(t, func() (string, error) { panic("supplier bug") })))
	if zerr.IsRetryable || !strings.Contains(zerr.Message, "panicked: supplier bug") {
		t.Errorf("got %q (retryable=%v), want a non-retryable panic error", zerr.Message, zerr.IsRetryable)
	}
	env.mustOpen(t, fedTable, newToken(t, (&countingSupplier{tokens: []string{fakeJWT(t, "a")}}).get))
}

// Concurrent creates on one token mint once (no stampede, no worker deadlock).
func TestFederatedConcurrentCreatesMintOnce(t *testing.T) {
	env := newFedEnv(t)
	supplier := &countingSupplier{tokens: []string{fakeJWT(t, "a")}, delay: 500 * time.Millisecond}
	token := newToken(t, supplier.get)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := env.sdk.CreateStreamWithFederatedToken(zerobus.TableProperties{TableName: fedTable}, token, jsonOpts())
			if err == nil {
				stream.Close()
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent create: %v", err)
		}
	}
	if c, n := supplier.calls.Load(), env.token.Count(); c != 1 || n != 1 {
		t.Errorf("supplier calls = %d, exchanges = %d; want 1 and 1", c, n)
	}
}

// Refresh re-invokes the supplier and exchanges its new token.
func TestFederatedRefreshReinvokesSupplier(t *testing.T) {
	// The SDK refreshes a short-lived token once less than min(300 s buffer, TTL/2)
	// remains, so a 2 s token is due for refresh after 1 s.
	env := newFedEnv(t, TokenReply{ExpiresIn: 2})
	jwt1, jwt2 := fakeJWT(t, "first"), fakeJWT(t, "second")
	supplier := &countingSupplier{tokens: []string{jwt1, jwt2}}
	token := newToken(t, supplier.get)

	env.mustOpen(t, fedTable, token)
	time.Sleep(1500 * time.Millisecond)
	env.mustOpen(t, fedTable, token)
	waitFor(t, "the refresh exchange", func() bool { return env.token.Count() >= 2 })

	reqs := env.token.Requests()
	if reqs[0].Form.Get("subject_token") != jwt1 || reqs[1].Form.Get("subject_token") != jwt2 {
		t.Error("the refresh must exchange the supplier's new token, not a cached one")
	}
	if c := supplier.calls.Load(); c < 2 {
		t.Errorf("supplier calls = %d, want at least 2", c)
	}
}

// An exchange 4xx is non-retryable with the server's reason; 5xx is retryable.
func TestFederatedExchangeErrorClassification(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		retryable bool
	}{
		{"401 policy mismatch", 401, false},
		{"400 bad request", 400, false},
		{"500 server error", 500, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script := make([]TokenReply, 20)
			for i := range script {
				script[i] = TokenReply{Status: tc.status, Body: `{"error":"federation policy mismatch"}`}
			}
			env := newFedEnv(t, script...)
			jwt := fakeJWT(t, "a")
			zerr := asZerobusError(t, env.open(t, fedTable, newToken(t, (&countingSupplier{tokens: []string{jwt}}).get)))
			if zerr.IsRetryable != tc.retryable {
				t.Errorf("retryable = %v, want %v (%s)", zerr.IsRetryable, tc.retryable, zerr.Message)
			}
			if !tc.retryable && !strings.Contains(zerr.Message, "federation policy mismatch") {
				t.Errorf("message %q lacks the server's reason", zerr.Message)
			}
			if strings.Contains(zerr.Message, jwt) {
				t.Error("error message leaks the IdP token")
			}
		})
	}
}

// The Arrow entry point sends the right exchange in both modes and shares the token.
func TestFederatedArrowStreams(t *testing.T) {
	for _, mode := range []string{"account_level", "workload_identity"} {
		t.Run(mode, func(t *testing.T) {
			_, serverURL, stop, err := StartMockArrowServer()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(stop)
			tokenMock := NewMockTokenServer()
			t.Cleanup(tokenMock.Close)
			sdk, err := zerobus.NewZerobusSdk(serverURL, tokenMock.URL)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(sdk.Free)

			jwt := fakeJWT(t, "arrow-"+mode)
			var opts []zerobus.FederatedTokenOption
			if mode == "workload_identity" {
				opts = append(opts, zerobus.WithClientID(spID))
			}
			supplier := &countingSupplier{tokens: []string{jwt}}
			token := newToken(t, supplier.get, opts...)
			for i := 0; i < 2; i++ {
				stream, err := sdk.CreateArrowStreamWithFederatedToken(
					arrowTestTable, makeSchemaIPC(testArrowSchema()), token, arrowOpts())
				if err != nil {
					t.Fatalf("create Arrow stream %d: %v", i, err)
				}
				stream.Close()
			}

			reqs := tokenMock.Requests()
			if len(reqs) != 1 || supplier.calls.Load() != 1 {
				t.Fatalf("exchanges = %d, supplier calls = %d; want 1 and 1 (the token must be shared)", len(reqs), supplier.calls.Load())
			}
			if reqs[0].Form.Get("subject_token") != jwt {
				t.Error("Arrow exchange subject_token differs from the supplier's token")
			}
			_, hasClientID := reqs[0].Form["client_id"]
			if wantID := mode == "workload_identity"; hasClientID != wantID ||
				(wantID && reqs[0].Form.Get("client_id") != spID) {
				t.Errorf("Arrow exchange client_id = %v, want present=%v", reqs[0].Form["client_id"], wantID)
			}
		})
	}
}

// The cgo.Handle the FFI owns is released once a stream is closed, or when a
// create fails. A leaked or never-freed handle would keep the token alive.
func TestFederatedTokenReleasedAfterStreams(t *testing.T) {
	env := newFedEnv(t)
	var released atomic.Int32
	func() {
		ok := newToken(t, (&countingSupplier{tokens: []string{fakeJWT(t, "a")}}).get)
		failing := newToken(t, func() (string, error) { return "", errors.New("idp down") })
		runtime.SetFinalizer(ok, func(*zerobus.FederatedToken) { released.Add(1) })
		runtime.SetFinalizer(failing, func(*zerobus.FederatedToken) { released.Add(1) })

		for i := 0; i < 2; i++ {
			stream, err := env.sdk.CreateStreamWithFederatedToken(zerobus.TableProperties{TableName: fedTable}, ok, jsonOpts())
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			stream.Close()
		}
		if _, err := env.sdk.CreateStreamWithFederatedToken(
			zerobus.TableProperties{TableName: fedTable}, failing, jsonOpts()); err == nil {
			t.Fatal("expected the failing create to fail")
		}
	}()
	waitFor(t, "both tokens to be released", func() bool {
		runtime.GC()
		return released.Load() == 2
	})
}
