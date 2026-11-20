//go:build testing

package zerobus

// WithInsecureTransportForTesting is TESTING ONLY (conformance harness). It
// disables TLS on the Zerobus data-plane connection, so the endpoint must be a
// bare "host:port" with no scheme. Bearer tokens and record data are sent in
// plaintext: never use it against a real endpoint.
//
// Gated behind the "testing" build tag to match the other SDKs' testing
// feature: build with -tags testing to expose it.
func WithInsecureTransportForTesting() Option {
	return func(c *sdkConfig) { c.insecureForTesting = true }
}
