package tests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"time"
)

// TokenReply scripts one response of the mock token endpoint. The zero value
// is a 200 with a distinct access token ("dbx-token-<n>") and expires_in 3600.
type TokenReply struct {
	Status    int           // HTTP status; 0 means 200
	ExpiresIn int           // seconds; 0 means 3600
	Body      string        // raw body for a non-200 reply
	Delay     time.Duration // delay before replying
}

// TokenRequest is one request the mock token endpoint received.
type TokenRequest struct {
	Method string
	Path   string
	Header http.Header
	Form   url.Values
}

// MockTokenServer is a loopback Unity Catalog token endpoint. It records every
// request and replies from a script, then with the default reply.
type MockTokenServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []TokenRequest
	script   []TokenReply
}

// NewMockTokenServer starts a mock token endpoint; its URL is the SDK's
// Unity Catalog URL.
func NewMockTokenServer(script ...TokenReply) *MockTokenServer {
	m := &MockTokenServer{script: script}
	m.Server = httptest.NewServer(http.HandlerFunc(m.serve))
	return m
}

func (m *MockTokenServer) serve(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	m.mu.Lock()
	n := len(m.requests)
	m.requests = append(m.requests, TokenRequest{
		Method: r.Method, Path: r.URL.Path, Header: r.Header.Clone(), Form: r.PostForm,
	})
	reply := TokenReply{}
	if len(m.script) > 0 {
		reply, m.script = m.script[0], m.script[1:]
	}
	m.mu.Unlock()

	if reply.Delay > 0 {
		time.Sleep(reply.Delay)
	}
	if reply.Status != 0 && reply.Status != http.StatusOK {
		w.WriteHeader(reply.Status)
		_, _ = w.Write([]byte(reply.Body))
		return
	}
	expiresIn := reply.ExpiresIn
	if expiresIn == 0 {
		expiresIn = 3600
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": fmt.Sprintf("dbx-token-%d", n),
		"token_type":   "Bearer",
		"expires_in":   expiresIn,
	})
}

// Requests returns a copy of every request received so far.
func (m *MockTokenServer) Requests() []TokenRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]TokenRequest(nil), m.requests...)
}

// Count returns the number of requests received so far.
func (m *MockTokenServer) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}
