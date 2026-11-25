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
- The regular JAR now also works with a protobuf-java 3.25 runtime that the application declares
  itself. The default runtime and the fat JAR still use protobuf-java 4.33.0.
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

- The published POM now declares `protobuf-java` 4.33.0 and `slf4j-api` 2.0.17 as dependencies of
  the regular JAR. Before, the POM left them out, so a Maven application that declared neither
  library got `NoClassDefFoundError`s, and one that relied on a transitive `protobuf-java` got
  whatever another dependency brought in. Now Maven's nearest-wins rule applies: a `protobuf-java`
  or `slf4j-api` that your application declares directly wins, but a version that arrives at
  depth 2 or deeper is replaced by the SDK's depth-1 version. Migration: if you need another
  version, declare it directly in your build (for example `protobuf-java` 3.25.x), and confirm the
  resolved version with `mvn dependency:tree`. Gradle does not apply nearest-wins and picks the
  highest version, so Gradle users who need 3.25.x must force it.

### Deprecations

### API Changes

- The JAR now includes the generated `InsertRecordsRequest` and
  `InsertRecordsResponse` classes from the shared service proto. They are wire
  types, not a supported API.
- New public API: `ZerobusSdk.builder()` returning `ZerobusSdkBuilder`; `TlsOptions` and
  `TlsOptions.Builder` (created with `TlsOptions.builder()`). `ZerobusSdkBuilder.tlsOptions(...)` takes
  a `TlsOptions`.
