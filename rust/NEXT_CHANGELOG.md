# NEXT CHANGELOG

## Release v3.2.0

### Major Changes

### New Features and Improvements

- `HeadersProvider` can now send binary gRPC metadata on both gRPC and Arrow Flight
  streams. Header names ending in `-bin` take a standard base64 value, with or
  without padding. The SDK decodes it and sends the bytes as sensitive binary
  metadata. Previously, these headers failed to send.
- Added `StreamBuilder::dictionary_handling(ArrowDictionaryHandling)` for Arrow
  Flight streams. The default, `Hydrate`, decodes dictionary-encoded columns on
  the client, as before. `Resend` sends them as Arrow IPC dictionaries. `Delta`
  also sends dictionaries, but when a batch's dictionary starts with the
  previous batch's values for that column, it sends only the new values.
  Zerobus does not support Arrow dictionaries yet, so keep the default. Until
  it does, `Resend` or `Delta` with dictionary-encoded columns makes stream
  creation fail with `ZerobusError::InvalidSchema`.
- Added `TlsSettings::with_ca_certificate_file` and
  `TlsSettings::with_client_identity_files` to load CA certificates and the
  mutual TLS client identity from PEM files, so rotated certificates take
  effect without a restart or a custom `TlsConfig`. A missing, unreadable,
  non-regular or oversized file gives a non-retryable
  `ZerobusError::InvalidArgument` with the path. Unusable content read from a
  file, such as a certificate that doesn't match the key, gives a retryable
  `ZerobusError::ChannelCreationError`.
- The SDK now calls `TlsConfig::tls_settings()` before it reuses a gRPC channel
  for a new stream or a recovery attempt, and opens a new channel when the
  settings changed. Rotated certificates now reach a shared channel
  (`connection_per_stream` off) and gRPC recovery, which used to keep the
  certificates the channel was opened with. If the settings can't be read or
  applied at that point, the SDK keeps the current channel and logs a warning.
  A gRPC stream now opens its first channel inside the recovery retry loop, as
  Arrow Flight streams already did, so a retryable error there is retried when
  recovery is enabled.

### Bug Fixes

- An endpoint that isn't a valid URI now gives a non-retryable
  `ZerobusError::InvalidZerobusEndpointError`, and an SDK identifier that isn't
  a valid `user-agent` header value gives a non-retryable
  `ZerobusError::InvalidArgument`. Both used to give a retryable
  `ChannelCreationError`, so Arrow Flight stream creation retried them until
  the recovery retries ran out.

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, schema generator, and API docs link to it instead
  of repeating cloud-specific URLs.
- The examples read their connection settings from the `ZEROBUS_SERVER_ENDPOINT`,
  `DATABRICKS_WORKSPACE_URL`, `ZEROBUS_TABLE_NAME`, `DATABRICKS_CLIENT_ID`, and
  `DATABRICKS_CLIENT_SECRET` environment variables instead of constants in the
  source.

### Internal Changes

- Moved the table, authentication, record format, and shared retry settings of
  `StreamBuilder` into a private builder core that other client builders can
  reuse. Stream behavior is unchanged.

### Breaking Changes

### Deprecations

### API Changes

- Added `ArrowDictionaryHandling`, `StreamBuilder::dictionary_handling`, and
  `ArrowStreamConfigurationOptions::dictionary_handling` (behind the
  `arrow-flight` feature).
