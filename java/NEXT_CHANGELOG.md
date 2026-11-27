# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

- This version requires a `libzerobus_jni` built from the same release, even if
  you don't use federated auth. If you load the library from
  `java.library.path`, rebuild it before upgrading: the SDK tries that path
  first and fails to load an older library instead of falling back to the
  bundled one.

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
- Add `ZerobusSdk.builder()` returning a `ZerobusSdkBuilder`, which sets the endpoints, application
  name, connection mode, and TLS options. The existing `ZerobusSdk` constructors still work and
  keep their defaults.
- Add `TlsOptions` to configure TLS: trust only the CA certificates you add
  (`trustSystemRoots(false)`), add CA certificates as PEM text or as files, present a client
  certificate for mutual TLS as PEM text or as files, and override the server domain name.
  Certificate files are read on each new connection, so rotated files take effect without a restart.
  Previously the SDK always used the operating system's CA certificates. These settings apply to the
  Zerobus gRPC connection only. OAuth and federated token requests to the workspace URL trust the
  operating system's CA certificates and the SDK's bundled public roots, ignore TlsOptions, and
  present no client certificate.
- `HeadersProvider` header names ending in `-bin` are now accepted. The value must be the standard
  base64 encoding of the bytes, and the SDK sends the decoded bytes as binary gRPC metadata. Names
  beginning with `grpc-` and ending in `-bin` are reserved and rejected.

- Behavior change (no API change): stream creation and recovery retries now back off
  exponentially with jitter instead of waiting a fixed `recoveryBackoffMs` between
  attempts (from the Rust core). The wait starts at `recoveryBackoffMs` and is never
  shorter than it. It doubles after each consecutive failed attempt, up to 15 s (or
  `recoveryBackoffMs` if larger), and a random jitter of up to the same amount is
  added, so a single wait can reach 30 s. With the defaults (2 s, 4 retries),
  retries wait 2–4 s, 4–8 s, 8–16 s, then 15–30 s, so a stream that keeps failing
  gives up after about 29–58 s of waiting instead of 8 s. Recovery of a gRPC stream
  after a failure now also waits before its first reconnect attempt (2–4 s by
  default), so streams the server drops at the same time don't all reconnect at
  once; a gRPC stream that keeps failing during recovery gives up after about
  44–88 s instead of 8 s. A server-initiated Arrow Flight stream rotation still
  waits `recoveryBackoffMs` on average (a random wait between half and 1.5 times
  it), and a gRPC close signal still reconnects immediately. Clients rejected at the
  same time, for example by the server's stream-creation rate limit, no longer all
  retry at the same moment. A backoff of 0 still retries immediately.

  To keep a total retry window close to the previous one, lower `recoveryRetries`.
  With the default 2 s base, 2 retries wait 6–12 s in total (14–28 s for gRPC
  recovery after a failure).

### Bug Fixes

- Fix `AckCallback` notifications being silently dropped after a callback throws. An exception from `onAck` or `onError` was left pending on the SDK thread that ran the callback, so every later callback on that thread, including callbacks of other streams, failed without reaching Java. The exception is now cleared and normally logged at ERROR level with its stack trace through the SDK's SLF4J logger (visible when an SLF4J binding is configured; if logging itself fails, the exception is dropped), and later callbacks are delivered. Callbacks also no longer leave the thrown exception and the `onError` message referenced by native code after they return.
- Fix the JVM aborting when the native library panics. A panic reaching the JNI boundary aborted the whole process. A panic during a native method call now surfaces as a `NonRetriableException`, and a panic while the library loads makes `System.load` fail with `UnsatisfiedLinkError`.
- Calling a blocking SDK or stream method, such as `flush()`, `close()`, `waitForOffset()`, or `isClosed()`, from an `AckCallback` now throws `IllegalStateException`. Previously it aborted the JVM. Hand such work to another thread instead.

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, proto generator docs, and API docs link to it
  instead of repeating cloud-specific URLs.

### Internal Changes

- Bumped the native JNI ABI version to 3 for the new federated stream-creation
  methods and the TLS-options constructor. Release a `jni/v*` build from this
  source before the Java release, and rebuild any laptop-built macOS dylibs that
  `release-java.yml` imports through `macos-binaries-release`.

### Breaking Changes

### Deprecations

### API Changes

- The JAR now includes the generated `InsertRecordsRequest` and
  `InsertRecordsResponse` classes from the shared service proto. They are wire
  types, not a supported API.
- New public API: `ZerobusSdk.builder()` returning `ZerobusSdkBuilder`; `TlsOptions` and
  `TlsOptions.Builder` (created with `TlsOptions.builder()`). `ZerobusSdkBuilder.tlsOptions(...)` takes
  a `TlsOptions`.
