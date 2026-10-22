/* One Zerobus ephemeral stream: the landing zone with its offsets, and the
 * supervisor, sender and receiver threads that carry it over the transport.
 * Backs the public zerobus_stream_t. */
#ifndef ZB_EPHEMERAL_STREAM_H
#define ZB_EPHEMERAL_STREAM_H

#include "internal/network/tls.h"
#include "zerobus/common.h"

typedef struct zb_ephemeral_stream zb_ephemeral_stream_t;

typedef struct {
    const char *table;
    const char *host;
    uint16_t port;
    bool plaintext;
    /* Required for TLS. Borrowed for the stream's lifetime. */
    const zb_tls_ctx_t *tls_context;
    /* The stream takes its own reference. */
    zerobus_headers_provider_t *headers;
    /* Records admitted but not yet acknowledged. A power of two, at least 4. */
    size_t max_inflight;
    bool recovery;
    uint32_t recovery_retries;
    uint64_t recovery_timeout_ms;
    uint64_t recovery_backoff_ms;
    uint64_t lack_of_ack_timeout_ms;
    uint64_t flush_timeout_ms;
} zb_ephemeral_stream_config_t;

/* Copies the configuration, starts the supervisor, and waits until the first
 * stream open succeeds or fails for good, retrying as recovery allows. */
zerobus_status_t
zb_ephemeral_stream_new(const zb_ephemeral_stream_config_t *config,
                        zb_ephemeral_stream_t **out_stream,
                        zerobus_error_t **out_error);

/* Copies each record into the landing zone, waiting while it is full. One
 * call is one request with one offset: a single record, or with count > 1 a
 * batch. *out_offset receives the offset. FAILED_PRECONDITION once closing;
 * the stream's failure status after it failed. */
zerobus_status_t zb_ephemeral_stream_ingest(
    zb_ephemeral_stream_t *stream, const zerobus_string_view_t *records,
    size_t count, zerobus_offset_t *out_offset, zerobus_error_t **out_error);

/* Waits until every record admitted before the call is acknowledged, the
 * stream fails, or the flush timeout passes. */
zerobus_status_t zb_ephemeral_stream_flush(zb_ephemeral_stream_t *stream,
                                           zerobus_error_t **out_error);

/* Reads the acknowledged offset into *out_offset without waiting, and returns
 * the failure status once the stream has failed. */
zerobus_status_t zb_ephemeral_stream_acked_offset(zb_ephemeral_stream_t *stream,
                                                  zerobus_offset_t *out_offset,
                                                  zerobus_error_t **out_error);

/* Stops admission, flushes, ends the call, and joins every thread. Every
 * caller gets the first close's result. */
zerobus_status_t zb_ephemeral_stream_close(zb_ephemeral_stream_t *stream,
                                           zerobus_error_t **out_error);

/* Stops at once without flushing, joins every thread, and frees. NULL-safe.
 * Requires exclusive access. */
void zb_ephemeral_stream_free(zb_ephemeral_stream_t *stream);

#endif /* ZB_EPHEMERAL_STREAM_H */
