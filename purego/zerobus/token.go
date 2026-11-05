package zerobus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/auth"
)

// OAuthToken is a Zerobus-scoped Databricks OAuth access token returned by
// GetToken.
//
// Its String and GoString methods redact AccessToken, so formatting it with
// %v, %s, %+v, or %#v never prints the token.
type OAuthToken struct {
	// AccessToken is the OAuth 2.0 access token, sent as "Bearer <AccessToken>".
	AccessToken string
	// ExpiresAt is when the token expires, measured from when the request was
	// sent so it never overstates the token's validity. It is the zero Time when
	// the server reported no lifetime.
	ExpiresAt time.Time
}

// String formats the token with AccessToken redacted.
func (t OAuthToken) String() string {
	expiresAt := "none"
	if !t.ExpiresAt.IsZero() {
		expiresAt = t.ExpiresAt.Format(time.RFC3339)
	}
	return "OAuthToken{AccessToken:<redacted> ExpiresAt:" + expiresAt + "}"
}

// GoString formats the token as Go syntax with AccessToken redacted.
func (t OAuthToken) GoString() string {
	return fmt.Sprintf("zerobus.OAuthToken{AccessToken:%q, ExpiresAt:%#v}", "<redacted>", t.ExpiresAt)
}

// GetToken mints a Zerobus-scoped Databricks OAuth token for tableName using
// the client-credentials grant, the same token CreateStream obtains for its
// streams. Use it to authenticate any client that takes an OAuth token
// callback, such as a Kafka producer using SASL/OAUTHBEARER against Zerobus.
//
// GetToken does not cache: every call mints a fresh token. Let the client
// decide when to call it, for example on connect or before ExpiresAt.
//
// workspaceURL is the Databricks workspace URL and workspaceID its workspace
// ID; see the [connection URL table]. tableName is "catalog.schema.table". ctx
// bounds the request, which also times out after 30 seconds.
//
// Invalid arguments fail with a non-retryable *Error before any request is
// sent. A network failure, timeout, or 5xx response is retryable; a 4xx or
// malformed response is not.
//
// [connection URL table]: https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id
func GetToken(
	ctx context.Context,
	workspaceURL, tableName, clientID, clientSecret, workspaceID string,
) (OAuthToken, error) {
	token, expiresAt, err := auth.MintToken(ctx, workspaceURL, tableName, clientID, clientSecret, workspaceID)
	if err != nil {
		var tokenErr *auth.TokenError
		return OAuthToken{}, &Error{
			Op:        "GetToken",
			cause:     err,
			retryable: errors.As(err, &tokenErr) && tokenErr.IsRetryable(),
		}
	}
	return OAuthToken{AccessToken: token, ExpiresAt: expiresAt}, nil
}
