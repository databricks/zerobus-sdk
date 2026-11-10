# NEXT CHANGELOG

## Release v1.6.0

### Major Changes

### New Features and Improvements

- Arrow Flight streams now compress IPC payloads with ZSTD by default. Leaving
  `ArrowStreamConfigurationOptions.ipcCompression` unset now means `IpcCompressionType.Zstd`
  instead of no compression. To keep sending uncompressed payloads, set it to the new
  `IpcCompressionType.None`.
- Added `sdk.streamBuilder()`, a builder-style way to configure a stream: set the table, the
  authentication (`oauth()` or `headersProvider()`), the record format (`json()`,
  `compiledProto()` or `avro()`) and any stream options, then call `build()`. `createStream()`
  is unchanged.
- Added multiplexed streams (Beta): `streamBuilder()...multiplexed(n).build()` opens `n`
  sub-streams (1 to 64) to one table and spreads records round-robin across them, for when one
  stream is the throughput bottleneck. Ingest calls resolve to unordered `bigint` message IDs;
  confirm with `flush()` or `waitForMessageId()`. An optional `ackCallback({ onAck, onError })`
  reports each acknowledgment, and `close()` resolves once every callback has run. JSON,
  Protocol Buffers and Avro are supported. See the README and
  `examples/proto/multiplexed.ts`.

### Bug Fixes

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, and API docs link to it instead of repeating
  cloud-specific URLs.
- The examples exit with an error naming the missing environment variable instead of
  falling back to placeholder connection values.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- `IpcCompressionType` gains `None` (`-1`), which disables IPC compression.
- `ZerobusSdk` gains `streamBuilder()`. New types `StreamBuilder`, `MultiplexedStreamBuilder`,
  `MultiplexedAckCallback` and the class `ZerobusMultiplexedStream` (Beta).
