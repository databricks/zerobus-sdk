# Changelog

## Unreleased

### New Features

- Streams now ingest: `zerobus_stream_builder_build` opens an EphemeralStream
  call, ingest queues records for a sender thread, a receiver thread applies
  cumulative acknowledgments, and flush and close wait for them.
- Recover a failed call by replaying unacknowledged records on a new call,
  on by default; `zerobus_stream_builder_set_recovery` turns it off.
- OAuth client credentials: the stream requests a token scoped to its table from
  the Unity Catalog endpoint and refreshes it before expiry.
- Headers providers: `zerobus_headers_provider_new` for fixed headers and
  `zerobus_custom_headers_provider_new` for a callback, set with
  `zerobus_stream_builder_set_headers_provider` instead of OAuth.
- `zerobus_stream_ingest_json_records` queues a batch as one request.
- `zerobus_stream_get_acked_offset` reads the acknowledged offset without
  waiting, so an event loop can learn when records are durable without
  blocking in flush.
- `zerobus_stream_builder_set_max_inflight_records` and
  `zerobus_stream_builder_set_flush_timeout_ms` bound unacknowledged records and
  flush waits; `zerobus_sdk_builder_set_tls_ca_file` sets the trusted CAs.

### Internal Changes

- Integrate the multiplexed gRPC wrapper and socket/TLS platform implementations
  with private per-call message channels and one I/O owner for all calls on a
  connection. Call completion and cancellation leave other calls running.
- Separate connection-only I/O state from transport-owned call state, channel
  callbacks and control requests; remove the intermediate I/O-call object.
- Accept a caller-owned TLS context shared across connections; transport owns
  only each connection's TLS session, leaving trust loading to its caller.
- Make the per-call channels lock-free, with wakeups armed only by a waiting
  side, and coalesce I/O output into full TLS records.
- Pack framed requests into reused 64 KiB transport buffers aligned to whole
  DATA frames; lend received messages until the next receive and free them on
  the I/O thread.
- Validate UTF-8 with a table automaton, and with AVX2 where available.
- Give semaphores an atomic fast path that takes no lock.
