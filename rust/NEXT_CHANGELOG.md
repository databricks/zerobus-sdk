# NEXT CHANGELOG

## Release v2.11.0

### Major Changes

### New Features and Improvements

- Added Rust-only multiplexed Arrow Flight streams via
  `.arrow(schema).multiplexed(n).build_arrow()`. Whole batches route round-robin
  with a mux-wide `max_inflight_batches` budget, lane-local recovery, and the
  existing mux poisoning and message-ID semantics.

- Added Avro support to `StreamBuilder::multiplexed(n)` (Beta) when the `avro`
  feature is enabled; every lane shares the configured writer schema.
- Added first-class external-IdP token federation (`FederatedTokenProvider`,
  `IdpTokenSupplier`) alongside the existing OAuth client-credentials path. It
  exchanges an external IdP token (for example an Entra ID token) for a
  Zerobus-scoped Databricks token via the RFC 8693 token-exchange grant, caches
  and refreshes it through the existing `TokenCache`, and supports both
  account-level federation (no `client_id`, identity synced via Automatic
  Identity Management) and workload identity federation (a service principal
  `client_id` with no secret). Opt in via `StreamBuilder::federated_auth(supplier,
  client_id)`, where `client_id` is `None` for account-level federation or the
  service principal id for workload identity. The client-credentials and
  token-exchange grants now share one request-shaping path, keeping them at
  parity. The cached lifetime of an exchanged token is additionally capped at the
  subject JWT's remaining life (`min(expires_in, exp - now)`), so it is never
  served past the point its subject token expired. The shared token cache is
  partitioned by identity so entries never collide — workload keys by its service
  principal `client_id`, account-level by the supplier's own identity — so two
  distinct identities driven from one SDK instance never serve each other's token
  on the same table, without the caller passing a partition key (clone one
  supplier across streams to share its cached token; pass a distinct supplier to
  isolate). Existing `oauth(...)` and `headers_provider(...)` paths are unchanged.

### Bug Fixes

- Arrow `ingest_batch` calls blocked on `max_inflight_batches` now fail as soon as
  `close()` starts instead of waiting for close to finish.

### Documentation

- Documented Arrow mux lifecycle, capacity, partial-ack recovery, and shared
  lane-local telemetry; added the `arrow_multiplexed` example.

- Added a multiplexed Avro example using loop-then-flush ingestion.

### Internal Changes

- Added the feature-gated persistent gRPC transport, durable wire offsets,
  resume-watermark reconciliation after a lost acknowledgment, and validation
  for setup responses, acknowledgment bounds, and offset overflow.

- Extracted a private transport-generic mux core while retaining Arrow's
  supervisor-owned close and recovery paths.

### Breaking Changes

### Deprecations

### API Changes

- Added `MultiplexedArrowStream` and `MultiplexedStreamBuilder::build_arrow()`
  behind the `arrow-flight` feature, with batch and IPC ingestion, message waits,
  flush, close, and unacknowledged-batch retrieval.
