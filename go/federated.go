package zerobus

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// IdpTokenSupplier returns the current token from an external identity provider,
// for example a Microsoft Entra ID or other OIDC JWT, for FederatedToken
// authentication.
//
// The SDK exchanges the returned token for a Zerobus-scoped Databricks token
// (RFC 8693 token exchange), caches the exchanged token, and refreshes it before
// it expires. The supplier is called only when a fresh Databricks token must be
// minted (on a cache miss or a proactive refresh), not on every request, so it
// may perform a network call to the identity provider.
//
// The SDK calls the supplier from internal SDK threads, including during stream
// recovery, and may call it concurrently for streams on different tables, so it
// must be safe for concurrent use.
//
// A returned error is treated as retryable, like a failed OAuth token request.
// Return a *ZerobusError with IsRetryable set to false (it may be wrapped) for a
// permanent failure that should stop recovery immediately. A panic is recovered
// and treated as a permanent failure.
type IdpTokenSupplier func() (string, error)

// FederatedToken authenticates a stream by federating an external identity
// provider token (for example from Microsoft Entra ID) instead of a Databricks
// OAuth client secret. Pass it to CreateStreamWithFederatedToken or
// CreateArrowStreamWithFederatedToken.
//
// The federation policy must already be configured in Databricks. Two modes are
// supported:
//   - Account-level federation (the default): no Databricks service principal.
//     The token's subject resolves to an identity synced into Databricks, for
//     example through automatic identity management.
//   - Workload identity federation (WithClientID): a Databricks service
//     principal with a federation policy and no secret, named by its client ID.
//
// A FederatedToken is immutable and safe for concurrent use. Each instance has
// its own token cache entry: streams created with the same FederatedToken share
// one exchanged token, while two instances never share a token, even with the
// same supplier. Reuse one instance across streams for the same identity. To
// switch identities, create a new instance.
//
// Example:
//
//	token, err := zerobus.NewFederatedToken(fetchEntraIDToken, zerobus.WithClientID(spClientID))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	stream, err := sdk.CreateStreamWithFederatedToken(
//	    zerobus.TableProperties{TableName: "catalog.schema.table"},
//	    token,
//	    options,
//	)
type FederatedToken struct {
	supplier IdpTokenSupplier
	clientID string
	// A random id fixed at construction partitions the SDK's token cache per
	// instance. It is never derived from an address, which can be reused.
	cacheIdentity string
}

// FederatedTokenOption configures a FederatedToken.
type FederatedTokenOption func(*FederatedToken) error

// WithClientID selects workload identity federation, which authenticates as the
// Databricks service principal with the given client ID. Without this option the
// token uses account-level federation.
func WithClientID(clientID string) FederatedTokenOption {
	return func(t *FederatedToken) error {
		if strings.TrimSpace(clientID) == "" {
			return errors.New("zerobus: clientID must not be empty")
		}
		t.clientID = clientID
		return nil
	}
}

// NewFederatedToken creates a FederatedToken that obtains external IdP tokens
// from supplier. It uses account-level federation unless WithClientID is given.
func NewFederatedToken(supplier IdpTokenSupplier, opts ...FederatedTokenOption) (*FederatedToken, error) {
	if supplier == nil {
		return nil, errors.New("zerobus: IdpTokenSupplier must not be nil")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("zerobus: generate federated token identity: %w", err)
	}
	t := &FederatedToken{supplier: supplier, cacheIdentity: "go:" + hex.EncodeToString(id[:])}
	for _, opt := range opts {
		if err := opt(t); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// String describes the federation mode without exposing any token.
func (t *FederatedToken) String() string {
	if t.clientID == "" {
		return "FederatedToken{accountLevel}"
	}
	return "FederatedToken{workloadIdentity, clientID=" + t.clientID + "}"
}

// idpTokenResult is the outcome of one supplier call, ready to cross the FFI.
type idpTokenResult struct {
	token        string
	errorMessage string // empty on success
	retryable    bool
}

// callIdpTokenSupplier invokes the supplier and classifies a failure. It never
// panics: a panic must not unwind into the Rust caller.
func callIdpTokenSupplier(supplier IdpTokenSupplier) (res idpTokenResult) {
	defer func() {
		if r := recover(); r != nil {
			res = idpTokenResult{errorMessage: fmt.Sprintf("IdpTokenSupplier panicked: %v", r)}
		}
	}()
	token, err := supplier()
	if err == nil {
		return idpTokenResult{token: token}
	}
	message := err.Error()
	if message == "" {
		message = "IdpTokenSupplier returned an error"
	}
	var zerr *ZerobusError
	retryable := !(errors.As(err, &zerr) && zerr != nil && !zerr.IsRetryable)
	return idpTokenResult{errorMessage: message, retryable: retryable}
}
