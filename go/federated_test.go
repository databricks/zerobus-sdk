package zerobus

import (
	"errors"
	"fmt"
	"runtime/cgo"
	"strings"
	"testing"
	"unsafe"
)

func okSupplier() (string, error) { return "idp-token", nil }

func TestNewFederatedTokenModes(t *testing.T) {
	account, err := NewFederatedToken(okSupplier)
	if err != nil {
		t.Fatal(err)
	}
	if account.clientID != "" || account.String() != "FederatedToken{accountLevel}" {
		t.Errorf("account-level token = %v (clientID %q)", account, account.clientID)
	}

	workload, err := NewFederatedToken(okSupplier, WithClientID("sp-client-id"))
	if err != nil {
		t.Fatal(err)
	}
	if workload.clientID != "sp-client-id" ||
		workload.String() != "FederatedToken{workloadIdentity, clientID=sp-client-id}" {
		t.Errorf("workload token = %v", workload)
	}
}

func TestNewFederatedTokenValidation(t *testing.T) {
	if _, err := NewFederatedToken(nil); err == nil || !strings.Contains(err.Error(), "must not be nil") {
		t.Errorf("nil supplier: err = %v", err)
	}
	for _, id := range []string{"", "   "} {
		if _, err := NewFederatedToken(okSupplier, WithClientID(id)); err == nil ||
			!strings.Contains(err.Error(), "clientID must not be empty") {
			t.Errorf("WithClientID(%q): err = %v", id, err)
		}
	}
}

func TestFederatedTokenCacheIdentityIsUniquePerInstance(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		token, err := NewFederatedToken(okSupplier)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(token.cacheIdentity, "go:") || len(token.cacheIdentity) != len("go:")+32 {
			t.Fatalf("unexpected cache identity %q", token.cacheIdentity)
		}
		if seen[token.cacheIdentity] {
			t.Fatalf("duplicate cache identity %q", token.cacheIdentity)
		}
		seen[token.cacheIdentity] = true
	}
}

func TestCallIdpTokenSupplierClassifiesResults(t *testing.T) {
	permanent := &ZerobusError{Message: "permanent failure", IsRetryable: false}
	cases := []struct {
		name          string
		supplier      IdpTokenSupplier
		wantToken     string
		wantMessage   string // substring; empty means success
		wantRetryable bool
	}{
		{"token", okSupplier, "idp-token", "", false},
		{"plain error is retryable", func() (string, error) { return "", errors.New("transient failure") },
			"", "transient failure", true},
		{"retryable ZerobusError", func() (string, error) {
			return "", &ZerobusError{Message: "throttled", IsRetryable: true}
		}, "", "throttled", true},
		{"non-retryable ZerobusError", func() (string, error) { return "", permanent },
			"", "permanent failure", false},
		{"wrapped non-retryable ZerobusError", func() (string, error) {
			return "", fmt.Errorf("fetch entra token: %w", permanent)
		}, "", "fetch entra token", false},
		{"panic is permanent", func() (string, error) { panic("boom") },
			"", "IdpTokenSupplier panicked: boom", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := callIdpTokenSupplier(tc.supplier)
			if tc.wantMessage == "" {
				if res.errorMessage != "" || res.token != tc.wantToken {
					t.Fatalf("got %+v, want token %q", res, tc.wantToken)
				}
				return
			}
			if !strings.Contains(res.errorMessage, tc.wantMessage) || res.retryable != tc.wantRetryable {
				t.Fatalf("got %+v, want message containing %q and retryable=%v",
					res, tc.wantMessage, tc.wantRetryable)
			}
		})
	}
}

// TestGoFreeIdpTokenSupplierReleasesHandle verifies the FFI-owned destroy path:
// goFreeIdpTokenSupplier must delete the cgo.Handle it is handed.
func TestGoFreeIdpTokenSupplierReleasesHandle(t *testing.T) {
	token, err := NewFederatedToken(okSupplier)
	if err != nil {
		t.Fatal(err)
	}
	handlePtr := newFederatedTokenHandle(token)
	handle := *(*cgo.Handle)(unsafe.Pointer(&handlePtr))
	if handle.Value().(*FederatedToken) != token {
		t.Fatal("handle should resolve to the token before free")
	}

	goFreeIdpTokenSupplier(handlePtr)
	goFreeIdpTokenSupplier(nil) // a nil handle is ignored

	defer func() {
		if recover() == nil {
			t.Fatal("expected handle.Value() to panic after goFreeIdpTokenSupplier")
		}
	}()
	_ = handle.Value()
}

func TestCreateWithFederatedTokenRejectsNilToken(t *testing.T) {
	sdk := &ZerobusSdk{ptr: unsafe.Pointer(&struct{ b byte }{})} // never dereferenced
	if _, err := sdk.CreateStreamWithFederatedToken(TableProperties{TableName: "c.s.t"}, nil, nil); err == nil ||
		!strings.Contains(err.Error(), "FederatedToken must not be nil") {
		t.Errorf("CreateStreamWithFederatedToken(nil): err = %v", err)
	}
	if _, err := sdk.CreateArrowStreamWithFederatedToken("c.s.t", []byte{1}, nil, nil); err == nil ||
		!strings.Contains(err.Error(), "FederatedToken must not be nil") {
		t.Errorf("CreateArrowStreamWithFederatedToken(nil): err = %v", err)
	}
}
