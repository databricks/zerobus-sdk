# NEXT CHANGELOG

## Release v3.2.0

### Major Changes

### New Features and Improvements

- JSON and protobuf gRPC connections now send HTTP/2 keepalive PINGs every 30
  seconds (10 second ack timeout, also while no stream is open) and enable TCP
  keepalive with a 30 second idle time. NAT gateways, load balancers, and
  firewalls with short idle timeouts no longer drop idle streams without
  notice, which showed up as `Server ack timeout` on the next record. Dead
  connections now fail after about 40 seconds instead of at the 60 second ack
  timeout. Arrow Flight streams are unchanged. TCP keepalive is not applied
  through a proxy connector.

- Arrow Flight streams with IPC compression now pad IPC buffers to a multiple
  of 8 bytes instead of 64. With compression, 64-byte alignment gives no
  benefit and only adds padding bytes to the payload. Uncompressed payloads
  keep 64-byte alignment.
- Zeroparser's `MessageRegistry` is now aligned to 128 bytes. When threads
  share one registry and clone its `Arc` for every record, the reference
  counts were on the same cache line as the registry fields that the parser
  reads, which made parsing several times slower on many threads.

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

### Breaking Changes

### Deprecations

### API Changes

- Added `GrpcStatus` and `GrpcCode`. Use them to inspect
  `ZerobusError::CreateStreamError` and `ZerobusError::StreamClosedError`.
- Added `TlsSettings`. It implements `TlsConfig`, so you can pass it to
  `ZerobusSdkBuilder::tls_config` in an `Arc`. Its `Debug` output does not print
  key or certificate material.
- Added `UcColumn::new`, `UcColumn::with_type_text`, `UcColumn::with_type_json`,
  `UcColumn::with_nullable`, `UcColumn::with_position`, and
  `UcTableSchema::new`.
- Added `RecordType`.
- Added `ZerobusSdkBuilder::http2_keepalive_interval`,
  `ZerobusSdkBuilder::http2_keepalive_timeout`, and
  `ZerobusSdkBuilder::tcp_keepalive`, and the constants
  `DEFAULT_HTTP2_KEEPALIVE_INTERVAL`, `DEFAULT_HTTP2_KEEPALIVE_TIMEOUT`, and
  `DEFAULT_TCP_KEEPALIVE`.
