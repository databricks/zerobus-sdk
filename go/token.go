package zerobus

import (
	"fmt"
	"time"
)

// OAuthToken is a Zerobus-scoped Databricks OAuth access token, returned by
// [GetToken]. Its String and GoString methods redact AccessToken.
type OAuthToken struct {
	// AccessToken is the bearer token.
	AccessToken string
	// ExpiresAt is when the token expires, measured from when the request was
	// sent. It is the zero time if the server reported no lifetime.
	ExpiresAt time.Time
}

// String implements fmt.Stringer without exposing AccessToken.
func (t OAuthToken) String() string {
	return fmt.Sprintf("OAuthToken{AccessToken: <redacted>, ExpiresAt: %v}", t.ExpiresAt)
}

// GoString implements fmt.GoStringer without exposing AccessToken.
func (t OAuthToken) GoString() string {
	return t.String()
}

// GetToken fetches a Zerobus-scoped OAuth token for use outside the SDK's own
// streams.
//
// It mints the same token the SDK uses for its streams (OAuth client
// credentials, scoped to writing tableName), for clients that take an OAuth
// token callback, e.g. a Kafka producer using SASL/OAUTHBEARER. Each call
// fetches a new token; there is no caching, so call it from the client's
// refresh hook. The call blocks until the token endpoint responds, for at most
// 30 seconds.
//
// tableName is "catalog.schema.table"; workspaceID is the numeric workspace ID.
// The returned error is a *ZerobusError: retryable if the token endpoint was
// unreachable or failed, non-retryable if the input is invalid or the endpoint
// rejected the request (for example, bad credentials or missing table
// privileges).
func GetToken(workspaceURL, tableName, clientID, clientSecret, workspaceID string) (OAuthToken, error) {
	accessToken, expiresAtMs, err := getToken(workspaceURL, tableName, clientID, clientSecret, workspaceID)
	if err != nil {
		return OAuthToken{}, err
	}
	token := OAuthToken{AccessToken: accessToken}
	if expiresAtMs != 0 {
		token.ExpiresAt = time.UnixMilli(int64(expiresAtMs))
	}
	return token, nil
}
