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
- Added personal access token (PAT) authentication (Private Preview) behind the new
  opt-in `pat-auth` feature. PAT authentication for Zerobus requires server-side
  enablement and is not available for Arrow Flight streams yet. Call
  `.personal_access_token(token)` on `StreamBuilder` or `PersistentStreamBuilder` to
  authenticate with a Databricks PAT created with the `zerobus` API scope. The SDK
  sends the token as given and never refreshes it, and never includes it in `Debug`
  output. An authentication rejection while creating the stream fails immediately
  instead of spending the one-shot credential-refresh retry.

### Bug Fixes

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
- Added `StreamBuilder::personal_access_token` and
  `PersistentStreamBuilder::personal_access_token` (behind `pat-auth`; the persistent
  builder also needs `eos`). An empty token is rejected with
  `ZerobusError::InvalidArgument`; as with the other authentication setters, the last
  call wins (Private Preview).
- Added `PatHeadersProvider` (behind `pat-auth`), a `HeadersProvider` that sends a
  personal access token (Private Preview).
- Added `HeadersProvider::can_refresh`, a defaulted method returning `true`. A provider
  with a fixed credential returns `false` to skip the one-shot auth retry during stream
  creation; `PatHeadersProvider` does. Existing implementations are unaffected.
