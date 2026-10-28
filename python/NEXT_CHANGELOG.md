# NEXT CHANGELOG

## Release v1.11.0

### Major Changes

### New Features and Improvements

- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `ArrowStreamConfigurationOptions.ipc_compression` defaults to
  `IPCCompression.ZSTD` instead of `IPCCompression.NONE`. To keep sending
  uncompressed payloads, pass `ipc_compression=IPCCompression.NONE`.

### Bug Fixes

- Updated the native extension's transitive `rustls-webpki` to 0.103.16 and
  `webpki-roots` to 1.0.11, which stop trusting the compromised eMudhra (emSign)
  CAs.
- The package now requires `certifi>=2026.10.19`. `generate_proto` sends OAuth
  client credentials through `requests`, which trusts the installed `certifi`
  bundle. This version drops the emSign ECC Root CA - G3 root.

### Documentation

### Internal Changes

- Updated `certifi` to 2026.10.19 and, on Python 3.10+, `pip` to 26.2.2 in
  `requirements-dev.lock`; both drop the emSign ECC Root CA - G3 root.

### Breaking Changes

- A `flush()` or `wait_for_offset()` timeout now closes the stream. This change comes from
  the Rust SDK 3.0.0 core. Recovery stops, and the SDK does not send the pending records
  again. A later `close()` sends nothing and raises the timeout error. `close()` on a failed
  record stream now raises the error that made it fail. Before, it returned without error.
  If an `ack_callback` is set, a timeout calls its `on_error()` for each unacknowledged
  offset.
  Migration: call `recreate_stream()` (or `recreate_arrow_stream()` for Arrow streams) on
  the closed stream. It resends the unacknowledged records from `get_unacked_batches()`.
  Do not retry `flush()` or `close()` on the same stream. An ingest call that fails
  because the stream is closing or closed did not admit its record. Send that record on
  the new stream. The server can already have accepted batches that were sent but not
  acknowledged, so a resend can duplicate those batches.

### Deprecations

### API Changes
