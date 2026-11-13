# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- Added external-IdP token federation (for example Microsoft Entra ID). Call
  `StreamBuilder.federatedAuth(FederatedToken)` to authenticate without a
  Databricks OAuth secret. The SDK exchanges the token from your
  `IdpTokenSupplier` for a Zerobus-scoped Databricks token (RFC 8693 token
  exchange), caches it, and refreshes it before it expires. Use
  `FederatedToken.accountLevel(supplier)` for account-level federation, or
  `FederatedToken.workloadIdentity(supplier, clientId)` for workload identity
  federation as a service principal. Works with JSON, protobuf, Avro, and Arrow
  streams, and `recreateStream` reuses the stream's `FederatedToken`. See
  `examples/json/FederatedAuthExample.java`.

### Bug Fixes

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, proto generator docs, and API docs link to it
  instead of repeating cloud-specific URLs.

### Internal Changes

- Bumped the native JNI ABI version to 2 for the new federated stream-creation
  methods. This SDK version needs a `libzerobus_jni` built from the same
  source, and rejects an older one when it loads.

### Breaking Changes

### Deprecations

### API Changes
