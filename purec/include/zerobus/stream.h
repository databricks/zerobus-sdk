/*
 * Zerobus Pure C SDK — stream builder, stream handle, and ingestion.
 *
 * Thread-safety: a stream and its builder are not thread-safe. Use one caller
 * thread per stream — do not call ingest/flush/close concurrently on the same
 * stream. (Internal synchronization arrives with the transport core.)
 */
#ifndef ZEROBUS_STREAM_H
#define ZEROBUS_STREAM_H

#include "zerobus/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Stream builder. Requires a fully qualified catalog.schema.table name and
 * OAuth client credentials. Streams are ephemeral gRPC JSON streams
 * authenticated with Unity Catalog OAuth. Credential copies are zeroed on free.
 *
 * The SDK passed to new() is borrowed and must outlive this builder and every
 * stream built from it.
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
 * The record must be non-empty and no larger than the payload limit. Success
 * means the record was copied and queued locally, not acknowledged by the
 * server, so the caller may reuse the bytes on return.
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
 * no durability guarantee. The SDK must still be alive.
 */
ZEROBUS_API void ZEROBUS_CALL zerobus_stream_free(zerobus_stream_t *stream);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* ZEROBUS_STREAM_H */
