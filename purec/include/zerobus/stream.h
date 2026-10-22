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
 * Stream builder. Requires a fully qualified catalog.schema.table name and
 * authentication: OAuth client credentials or a headers provider. Streams are
 * ephemeral gRPC JSON streams. Credential copies are zeroed on free.
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

/* Authenticate with Unity Catalog OAuth client credentials, minting tokens
 * scoped to the table. Replaces a headers provider set before. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_builder_set_oauth(
    zerobus_stream_builder_t *builder, zerobus_string_view_t client_id,
    zerobus_string_view_t client_secret, zerobus_error_t **out_error);

/* Send the provider's headers, such as authorization, instead of OAuth. The
 * builder and its streams keep the provider alive. Replaces OAuth credentials
 * set before. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_headers_provider(
    zerobus_stream_builder_t *builder, zerobus_headers_provider_t *provider,
    zerobus_error_t **out_error);

/*
 * The most records, or batches, admitted but not yet acknowledged; ingest
 * waits while it is reached. Rounded up to a power of two, at least 4.
 * Default 1,048,576.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_max_inflight_records(
    zerobus_stream_builder_t *builder, size_t max_records,
    zerobus_error_t **out_error);

/*
 * Whether a retryable failure reopens the stream and resends unacknowledged
 * records. Delivery is at least once: resent records may be duplicated.
 * Default true.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_recovery(zerobus_stream_builder_t *builder,
                                    bool enabled, zerobus_error_t **out_error);

/* How long flush and close wait for acknowledgments. Default 300,000 ms. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_flush_timeout_ms(zerobus_stream_builder_t *builder,
                                            uint64_t timeout_ms,
                                            zerobus_error_t **out_error);

/*
 * Validates configuration and opens the stream: resolves the headers,
 * connects, and creates the server-side stream, retrying retryable failures
 * while recovery is enabled. The builder is not consumed and may be reused or
 * freed.
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
 * return. Waits while the in-flight limit is reached.
 *
 * On success *out_offset, when non-NULL, receives the record's stream offset —
 * a handle to wait on later.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_ingest_json_record(
    zerobus_stream_t *stream, zerobus_string_view_t json_record,
    zerobus_offset_t *out_offset, zerobus_error_t **out_error);

/*
 * Ingest records as one batch: one request, one offset, acknowledged
 * together. Each record follows the rules of
 * zerobus_stream_ingest_json_record, and the batch must fit the payload limit.
 * An empty batch admits nothing and sets *out_offset to -1.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_ingest_json_records(
    zerobus_stream_t *stream, const zerobus_string_view_t *json_records,
    size_t record_count, zerobus_offset_t *out_offset,
    zerobus_error_t **out_error);

/*
 * Wait until the server acknowledges every record queued before this call,
 * the stream fails, or the flush timeout passes.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_flush(zerobus_stream_t *stream, zerobus_error_t **out_error);

/*
 * The highest offset the server has acknowledged, without waiting: every
 * offset at or below it is durable. -1 before the first acknowledgment.
 * Compare it with offsets returned by ingest to learn when records are
 * durable from an event loop that must not block in flush. *out_offset is
 * written on every call with a valid stream, including when the stream has
 * failed, which returns the failure status.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_get_acked_offset(
    zerobus_stream_t *stream, zerobus_offset_t *out_offset,
    zerobus_error_t **out_error);

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
