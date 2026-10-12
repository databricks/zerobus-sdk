/* Passive gRPC-over-HTTP/2 client for one call. An I/O owner drives the
 * nghttp2 session; everything here runs on the owner's thread. */
#ifndef ZB_NETWORK_GRPC_H
#define ZB_NETWORK_GRPC_H

#include <nghttp2/nghttp2.h>

#include "message.h"
#include "zerobus/common.h"

/* An incoming message being assembled across DATA chunks. */
typedef struct {
    uint8_t prefix[5];
    size_t prefix_used;
    size_t payload_used;
    zb_message_t *message;
} zb_grpc_decoder_t;

typedef struct {
    zerobus_string_view_t authority; /* host[:port]; borrowed, init only. */
    bool tls;

    size_t max_header_bytes;
    size_t max_message_bytes;

    void *context; /* For the callbacks, valid until destroy. */

    /* Callbacks run inside the owner's calls into nghttp2 and this component,
     * and must not re-enter either. */

    /* Returns the next message to send and transfers ownership of it. Returns
     * NULL if none is queued yet. Sets *eof with NULL only when no message will
     * ever follow. Required. */
    zb_message_t *(*pull_send)(void *context, bool *eof);
    /* Receives each complete message in order. On true takes ownership, on
     * false refuses it for now, pausing input until zb_grpc_retry_receive
     * offers it again. Required. */
    bool (*on_message)(void *context, zb_message_t *message);
    /* Receives the call's final result exactly once. Result is borrowed,
     * result->message (may be NULL) is transferred. Optional. */
    void (*on_complete)(void *context, const zb_rpc_result_t *result);
} zb_grpc_config_t;

typedef struct {
    /* Session and call lifecycle. */
    nghttp2_session *session;
    zb_grpc_config_t config; /* authority cleared after init. */
    int32_t id;
    bool completed;
    bool request_closed;
    zerobus_status_t failure;

    /* Request: framing outgoing messages into DATA frames. */
    zb_message_t *sending;
    uint8_t send_prefix[5];
    size_t send_prefix_used;
    size_t send_payload_used;

    /* Response: validating the header block being parsed. */
    size_t header_bytes;
    uint32_t block_http_status;
    bool content_type_seen;
    bool content_type_valid;

    /* Response: the call's result, reported by on_complete. */
    uint32_t http_status; /* Of the response headers (first non-1xx block). */
    bool grpc_status_seen;
    uint32_t grpc_status;
    char *grpc_message;

    /* Response: decoding messages and holding a refused one. */
    zb_grpc_decoder_t decoder;
    bool paused; /* on_message refused, see zb_grpc_retry_receive. */
    /* Rest of the paused chunk, borrowed from the owner's input buffer. */
    const uint8_t *remainder;
    size_t remainder_len;
} zb_grpc_t;

/* Initializes the session and queues the call, nothing is sent until the
 * owner's next nghttp2_session_mem_send. *grpc must be zeroed; config, path and
 * headers are borrowed only for the duration of this call. On failure *grpc
 * is left destroyed and on_complete is not called. */
zerobus_status_t zb_grpc_init(zb_grpc_t *grpc, const zb_grpc_config_t *config,
                              zerobus_string_view_t path,
                              const zb_metadata_t *headers,
                              size_t header_count);

/* Completes an unfinished call as CANCELLED and frees everything. Accepts
 * zeroed or already destroyed state. */
void zb_grpc_destroy(zb_grpc_t *grpc);

/* For a fatal connection failure: completes the call with status and sets
 * failure. Only the first abort counts. Sends nothing. NULL safe. */
void zb_grpc_abort(zb_grpc_t *grpc, zerobus_status_t status);

/* While paused, offers the refused message to on_message again, then decodes
 * the rest of its chunk, otherwise does nothing. */
zerobus_status_t zb_grpc_retry_receive(zb_grpc_t *grpc);

#endif /* ZB_NETWORK_GRPC_H */
