/*
 * Zerobus Pure C SDK — stream builder, stream handle, and ingestion.
 */
#ifndef ZEROBUS_STREAM_H
#define ZEROBUS_STREAM_H

#include "zerobus/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Ingest, flush, and close may run concurrently on the same stream, using
 * separate output/error slots. Builders require exclusive access. Freeing a
 * stream must not race any use of it.
 *
 * Stream builder. Requires a fully qualified catalog.schema.table name.
 * Streams are ephemeral gRPC JSON streams. Building one requires either OAuth
 * client credentials or a headers provider. For a table that needs no
 * authentication, set a static headers provider with no headers. Credential
 * copies are zeroed on free.
 *
 * The builder and each built stream retain their own SDK reference, so the
 * application may release its SDK reference after new() succeeds.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_builder_new(
    zerobus_sdk_t *sdk, zerobus_stream_builder_t **out_builder,
    zerobus_error_t **out_error);

ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_builder_set_table(
    zerobus_stream_builder_t *builder, zerobus_string_view_t table_name,
    zerobus_error_t **out_error);

/* Authenticates with OAuth client credentials. Replaces a headers provider. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_builder_set_oauth(
    zerobus_stream_builder_t *builder, zerobus_string_view_t client_id,
    zerobus_string_view_t client_secret, zerobus_error_t **out_error);

/* Ingest requests queued or awaiting acknowledgement before ingest blocks.
 * Must be in range [1, 2^24]. Default 2^16. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_max_inflight_requests(
    zerobus_stream_builder_t *builder, size_t max_inflight_requests,
    zerobus_error_t **out_error);

/* Largest total record size one ingest call accepts, in bytes. Must be in range
 * [1, 2^31 - 65]. Default 10 MiB - 64 KiB, just under the server's request
 * limit, so a larger value only helps if the server accepts larger requests. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_max_ingest_payload_bytes(
    zerobus_stream_builder_t *builder, size_t max_bytes,
    zerobus_error_t **out_error);

/* Whether the stream reconnects automatically after a retryable failure.
 * Default true. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_recovery(zerobus_stream_builder_t *builder,
                                    bool enabled, zerobus_error_t **out_error);

/* The timeouts and waits below are in milliseconds: 0 means at once and
 * UINT64_MAX means no limit. */

/* Time limit for each recovery attempt. Default 15 seconds. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_recovery_timeout_ms(
    zerobus_stream_builder_t *builder, uint64_t timeout_ms,
    zerobus_error_t **out_error);

/* Wait between recovery attempts. Default 2 seconds. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_recovery_backoff_ms(
    zerobus_stream_builder_t *builder, uint64_t backoff_ms,
    zerobus_error_t **out_error);

/* Recovery attempts before the stream fails. Default 4. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_recovery_retries(zerobus_stream_builder_t *builder,
                                            uint32_t retries,
                                            zerobus_error_t **out_error);

/* How long records may stay unacknowledged before the stream is treated as
 * failed and recovery starts. Must be non-zero. Default 60 seconds. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_server_lack_of_ack_timeout_ms(
    zerobus_stream_builder_t *builder, uint64_t timeout_ms,
    zerobus_error_t **out_error);

/* Time limit for flush. Default 300 seconds. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_flush_timeout_ms(zerobus_stream_builder_t *builder,
                                            uint64_t timeout_ms,
                                            zerobus_error_t **out_error);

/* After the server announces it will close the stream, how long to wait for
 * in-flight acknowledgements, capped by the duration the server gives. Default
 * UINT64_MAX, which waits the server's full duration. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_stream_paused_max_wait_time_ms(
    zerobus_stream_builder_t *builder, uint64_t wait_ms,
    zerobus_error_t **out_error);

/*
 * Authenticates with the headers of a provider, see zerobus/headers_provider.h.
 * Replaces OAuth client credentials. The builder and every stream built from
 * it with this provider hold their own reference, so the caller may free the
 * provider once this returns.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_headers_provider(
    zerobus_stream_builder_t *builder, zerobus_headers_provider_t *provider,
    zerobus_error_t **out_error);

/*
 * Validates configuration and opens the stream: gets the headers, connects,
 * and creates the server-side stream, retrying retryable failures while
 * recovery is enabled. The builder is not consumed and may be reused or freed.
 *
 * Streams that share one connection (see
 * zerobus_sdk_builder_set_connection_per_stream) are not implemented yet:
 * build returns ZEROBUS_STATUS_UNIMPLEMENTED for them.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_builder_build(
    const zerobus_stream_builder_t *builder, zerobus_stream_t **out_stream,
    zerobus_error_t **out_error);

ZEROBUS_API void ZEROBUS_CALL
zerobus_stream_builder_free(zerobus_stream_builder_t *builder);

/*
 * Ingest one JSON record, sent verbatim (not reshaped to the table schema).
 * The record must be non-empty valid UTF-8, without raw NUL bytes, and no
 * larger than the payload limit. Success means the record was copied and queued
 * locally, not acknowledged by the server, so the caller may reuse the bytes on
 * return. Waits while max in-flight requests are queued or unacknowledged.
 *
 * On success *out_offset, when non-NULL, receives the record's stream offset —
 * a handle to wait on later.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_ingest_json_record(
    zerobus_stream_t *stream, zerobus_string_view_t json_record,
    zerobus_offset_t *out_offset, zerobus_error_t **out_error);

/*
 * Ingest records as one batch: one request with one offset, acknowledged
 * together. Prefer it over single records where throughput matters, since
 * each call costs one request. Each record follows the rules of
 * zerobus_stream_ingest_json_record, and together they must not exceed the
 * payload limit. A batch counts as one request towards max in-flight
 * requests. An empty batch admits nothing and sets *out_offset, when
 * non-NULL, to -1. json_records may be NULL when record_count is 0.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_ingest_json_records(
    zerobus_stream_t *stream, const zerobus_string_view_t *json_records,
    size_t record_count, zerobus_offset_t *out_offset,
    zerobus_error_t **out_error);

/*
 * Wait until the server acknowledges offset, an offset an ingest on this
 * stream returned, and so every record before it, as flush does: until the
 * stream fails or the flush timeout passes. Any other offset returns
 * INVALID_ARGUMENT, and after close FAILED_PRECONDITION.
 *
 * Each wait costs a round trip to the server, so waiting after every ingest
 * caps throughput at one record per round trip. Use it only where a record
 * must be acknowledged before going on; otherwise ingest all, then flush once.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_wait_for_offset(
    zerobus_stream_t *stream, zerobus_offset_t offset,
    zerobus_error_t **out_error);

/*
 * Wait until the server acknowledges every record queued before this call,
 * the stream fails, or the flush timeout passes.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_flush(zerobus_stream_t *stream, zerobus_error_t **out_error);

/*
 * Stop ingestion, flush pending records, and tear down the stream. Idempotent:
 * every call returns the first close's result. If the flush fails, teardown
 * still happens and the error is returned. After close, ingest and flush
 * return FAILED_PRECONDITION.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_close(zerobus_stream_t *stream, zerobus_error_t **out_error);

/*
 * Release the handle. If the stream was not closed, tears down best-effort with
 * no durability guarantee.
 */
ZEROBUS_API void ZEROBUS_CALL zerobus_stream_free(zerobus_stream_t *stream);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* ZEROBUS_STREAM_H */
