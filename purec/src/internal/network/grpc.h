/* Passive gRPC-over-HTTP/2 client for any number of calls on one connection.
 * An I/O owner drives the nghttp2 session; everything here runs on the
 * owner's thread. */
#ifndef ZB_NETWORK_GRPC_H
#define ZB_NETWORK_GRPC_H

#include <nghttp2/nghttp2.h>

#include "grpc_utils.h"
#include "message.h"
#include "zerobus/common.h"

typedef struct zb_grpc zb_grpc_t;
typedef struct zb_grpc_call zb_grpc_call_t;

/* Connection settings, shared by all calls. */
typedef struct {
    zerobus_string_view_t authority; /* host[:port]; copied by init. */
    bool tls;

    size_t max_header_bytes;
    size_t max_message_bytes;
} zb_grpc_config_t;

/* One call's settings. */
typedef struct {
    /* Borrowed only for the duration of zb_grpc_call_open. */
    zerobus_string_view_t path;
    const zb_metadata_t *headers;
    size_t header_count;

    void *context; /* For the callbacks, valid until the call completes. */

    /* Callbacks run inside the owner's calls into nghttp2 and this component,
     * and must not re-enter either. */

    /* Returns the next message to send and transfers ownership of it. Returns
     * NULL if none is queued yet. Sets *eof with NULL only when no message will
     * ever follow. Required. */
    zb_message_t *(*pull_send)(void *context, bool *eof);
    /* Takes back each message pull_send returned, once it is sent or dropped
     * at completion. NULL frees it. Optional. */
    void (*release_send)(void *context, zb_message_t *message);
    /* Receives each complete message in order. On true takes ownership, on
     * false refuses it for now, pausing input for every call until
     * zb_grpc_retry_receive offers it again. Required. */
    bool (*on_message)(void *context, zb_message_t *message);
    /* Receives the call's final result exactly once. The call is already
     * detached, so the owner may free it from here on. Result is borrowed,
     * result->message (may be NULL) is transferred. Optional. */
    void (*on_complete)(void *context, const zb_rpc_result_t *result);
} zb_grpc_call_config_t;

struct zb_grpc_call {
    /* Call lifecycle. */
    zb_grpc_t *grpc;
    zb_grpc_call_t *next;         /* In the session's list of open calls. */
    zb_grpc_call_config_t config; /* Callbacks only; no request fields. */
    int32_t id;
    bool completed;
    bool request_closed;

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

    /* Response: decoding messages. */
    zb_grpc_decoder_t decoder;
};

struct zb_grpc {
    /* Session lifecycle. */
    nghttp2_session *session;
    zb_grpc_config_t config; /* authority views the copy below. */
    char *authority;         /* Owned copy. */
    zerobus_status_t failure;
    zb_grpc_call_t *calls; /* Open calls. */

    /* Receive pause: one refused message stops input for every call. */
    bool paused;                 /* See zb_grpc_retry_receive. */
    zb_grpc_call_t *paused_call; /* Holds it; NULL once that call completes. */
    /* Rest of the paused chunk, borrowed from the owner's input buffer. */
    const uint8_t *remainder;
    size_t remainder_len;
};

/* Initializes the session. *grpc must be zeroed. Config is borrowed only for
 * the duration of this call. On failure *grpc is left destroyed. */
zerobus_status_t zb_grpc_init(zb_grpc_t *grpc, const zb_grpc_config_t *config);

/* Completes every open call as CANCELLED and frees the session. Accepts zeroed
 * or already destroyed state. */
void zb_grpc_destroy(zb_grpc_t *grpc);

/* For a fatal connection failure: completes every open call with status and
 * sets failure. Only the first abort counts. Sends nothing. NULL safe. */
void zb_grpc_abort(zb_grpc_t *grpc, zerobus_status_t status);

/* While paused, offers the refused message to on_message again, then decodes
 * the rest of its chunk, otherwise does nothing. */
zerobus_status_t zb_grpc_retry_receive(zb_grpc_t *grpc);

/* Queues a call on the session. It opens on the wire with the owner's next
 * nghttp2_session_mem_send. *call must be zeroed or completed, and stay in
 * place until it completes. Config is borrowed only for the duration of this
 * call. On failure the call is not opened and on_complete is not called. */
zerobus_status_t zb_grpc_call_open(zb_grpc_t *grpc, zb_grpc_call_t *call,
                                   const zb_grpc_call_config_t *config);

/* Completes an open call as CANCELLED and resets its stream; other calls
 * continue. Does nothing for a zeroed or completed call. */
void zb_grpc_call_cancel(zb_grpc_call_t *call);

#endif /* ZB_NETWORK_GRPC_H */
