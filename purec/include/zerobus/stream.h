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
 * OAuth client credentials. Streams are ephemeral gRPC JSON streams
 * authenticated with Unity Catalog OAuth. Credential copies are zeroed on free.
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

ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_builder_set_oauth(
    zerobus_stream_builder_t *builder, zerobus_string_view_t client_id,
    zerobus_string_view_t client_secret, zerobus_error_t **out_error);

/* Records queued or awaiting acknowledgement before ingest blocks. Must be a
 * power of two from 4 to 2^30. Default 2^20. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_max_inflight_requests(
    zerobus_stream_builder_t *builder, size_t max_inflight_requests,
    zerobus_error_t **out_error);

/* Whether the stream reconnects automatically after a retryable failure.
 * Default true. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_recovery(zerobus_stream_builder_t *builder,
                                    bool enabled, zerobus_error_t **out_error);

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
 * failed and recovery starts. Default 60 seconds. */
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
 * in-flight acknowledgements, capped by the duration the server gives. 0 does
 * not wait. Default UINT64_MAX, which waits the server's full duration. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_stream_paused_max_wait_time_ms(
    zerobus_stream_builder_t *builder, uint64_t wait_ms,
    zerobus_error_t **out_error);

/* Largest record ingest accepts, in bytes. Default 10 MiB - 64 KiB, just under
 * the server's request limit. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_builder_set_max_ingest_payload_bytes(
    zerobus_stream_builder_t *builder, size_t max_bytes,
    zerobus_error_t **out_error);

/*
 * Validates configuration and (once implemented) opens the initial
 * authenticated connection. The builder is not consumed and may be reused or
 * freed.
 *
 * Not implemented yet: validates configuration and returns a usable handle
 * without opening a connection.
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
 * return.
 *
 * On success *out_offset, when non-NULL, receives the record's stream offset —
 * a handle to wait on later.
 *
 * Not implemented yet: validates its inputs, then returns
 * ZEROBUS_STATUS_UNIMPLEMENTED until the networking core lands.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_stream_ingest_json_record(
    zerobus_stream_t *stream, zerobus_string_view_t json_record,
    zerobus_offset_t *out_offset, zerobus_error_t **out_error);

/*
 * Wait until the server acknowledges every record queued before this call.
 *
 * Not implemented yet: returns ZEROBUS_STATUS_UNIMPLEMENTED until the
 * networking core lands.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_stream_flush(zerobus_stream_t *stream, zerobus_error_t **out_error);

/*
 * Stop ingestion, flush pending records, and tear down the stream. Idempotent.
 * If the flush fails, teardown still happens and the error is returned. After
 * close, ingest and flush return FAILED_PRECONDITION.
 *
 * Not implemented yet: only marks the stream closed, no flush or teardown until
 * the networking core lands.
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
