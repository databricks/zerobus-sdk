/* Worker-facing generic bidirectional gRPC transport. Private to the SDK.
 * One transport owns a connection and its I/O thread; calls share that owner.
 */
#ifndef ZB_NETWORK_TRANSPORT_H
#define ZB_NETWORK_TRANSPORT_H

#include "internal/platform/concurrency.h"
#include "message.h"
#include "tls.h"

typedef struct zb_transport zb_transport_t;
typedef struct zb_transport_call zb_transport_call_t;

typedef enum { ZB_RECEIVE_MESSAGE, ZB_RECEIVE_ENDED } zb_receive_event_kind_t;

typedef struct {
    zb_receive_event_kind_t kind;
    /* MESSAGE lends data to the receiver until its next recv or the call's
     * free. NULL data is valid for len == 0. ENDED has no data. */
    uint8_t *data;
    size_t len;
    /* ENDED: grpc_message is borrowed until its call is freed. */
    zerobus_status_t status;
    bool has_grpc_status;
    uint32_t grpc_status;
    uint32_t http_status;
    uint32_t http2_error;
    const char *grpc_message;
} zb_receive_event_t;

typedef struct {
    const char *host; /* DNS name or unbracketed IP; not a URL. */
    uint16_t port;    /* Zero chooses 443, or 80 for explicit plaintext. */
    bool plaintext;   /* Explicit h2c prior knowledge; never a TLS fallback. */
    /* Required for TLS, forbidden for plaintext. Caller-owned and borrowed
     * until open returns; established sessions retain the native context. */
    const zb_tls_ctx_t *tls_context;
    /* Zero chooses defaults: message 16 MiB, headers 64 KiB, each call's TX/RX
     * queues 256 messages each. Queue capacity counts messages, not bytes.
     * TX sending and RX decoding may each retain one additional message
     * outside their queue.
     * Empty messages consume a slot. Aggregate memory budgeting is deferred. */
    size_t max_message_bytes;
    size_t max_header_bytes;
    size_t send_queue_messages;
    size_t receive_queue_messages;
} zb_transport_config_t;

/* Resolves DNS on this caller, then starts the connection's I/O thread and
 * waits for TCP/TLS readiness. Opens no RPC. The caller creates TLS settings
 * with zb_tls_ctx_new and may share them across connection opens.
 * libc DNS cannot be interrupted; deadline is checked before/after lookup.
 * TCP/TLS setup is nonblocking and deadline-bound. TLS verifies chain +
 * host/IP and requires ALPN h2. Inputs are borrowed only for this call.
 * Output arguments throughout this API change only on success. */
zerobus_status_t zb_transport_open(const zb_transport_config_t *config,
                                   zb_deadline_t deadline,
                                   zb_transport_t **out_transport);

/* Opens a call through the connection's I/O thread, which copies path/headers
 * before returning. Does not wait for server acceptance: RPC results arrive
 * through recv. Calls have independent queues and completion results. Opening
 * calls may overlap other call operations and connection shutdown. A deadline
 * bounds admission, not the lifetime of the opened call. */
zerobus_status_t
zb_transport_call_open(zb_transport_t *transport, zerobus_string_view_t path,
                       const zb_metadata_t *headers, size_t header_count,
                       zb_deadline_t deadline, zb_transport_call_t **out_call);

/* One sender at a time. Queue admission, not delivery or acknowledgment. On OK
 * takes ownership of the malloc-owned payload; on failure the caller retains
 * it. Waits for capacity until deadline. May overlap recv/cancel/shutdown.
 * Different calls may be used by different senders and receivers. */
zerobus_status_t zb_transport_send(zb_transport_call_t *call, uint8_t *data,
                                   size_t len, zb_deadline_t deadline);
/* Bytes of the prefix gRPC puts before each message. */
#define ZB_TRANSPORT_PREFIX 5u

/* Writes the prefix of a len-byte message at out. */
void zb_transport_write_prefix(uint8_t *out, uint32_t len);

/* Bytes in each buffer from zb_transport_buffer. */
#define ZB_TRANSPORT_CHUNK 65536u

/* Most bytes a DATA frame carries; ZB_TRANSPORT_CHUNK is a multiple of it.
 * Framed sends that keep whole multiples of it queued fill every frame. */
#define ZB_TRANSPORT_FRAME 16384u

/* Returns a ZB_TRANSPORT_CHUNK-byte buffer for zb_transport_send_framed,
 * reusing one the call has sent when possible. NULL when out of memory. Only
 * the call's sender may call this. */
uint8_t *zb_transport_buffer(zb_transport_call_t *call);

/* Frees a buffer from zb_transport_buffer that was not sent. NULL-safe. */
void zb_transport_buffer_free(uint8_t *buffer);

/* Like zb_transport_send, for len bytes of a buffer from zb_transport_buffer
 * holding messages that each start with their prefix, as written by
 * zb_transport_write_prefix. Sent as is, so one send can carry many messages,
 * and a message may continue in the next send. */
zerobus_status_t zb_transport_send_framed(zb_transport_call_t *call,
                                          uint8_t *buffer, size_t len,
                                          zb_deadline_t deadline);
/* One receiver at a time. Drains accepted messages before ENDED; subsequent
 * recv repeats ENDED. A wait timeout does not cancel the RPC. */
zerobus_status_t zb_transport_recv(zb_transport_call_t *call,
                                   zb_deadline_t deadline,
                                   zb_receive_event_t *out_event);
/* Call after the last send completes; must not race a send. Further sends
 * are refused, queued messages drain in order, and receiving continues.
 */
zerobus_status_t zb_transport_half_close(zb_transport_call_t *call);
/* Cancel only this call and wake its blocked workers; other calls continue.
 * Returns once the I/O owner no longer references the call's gRPC state. */
zerobus_status_t zb_transport_cancel(zb_transport_call_t *call);
/* NULL-safe. Cancels, detaches, and frees the call and its queued messages.
 * Requires exclusive access to this call: stop/join its workers first. */
void zb_transport_call_free(zb_transport_call_t *call);
/* Abort, join the I/O thread, and close network resources. Queued receives and
 * each call's terminal result remain readable. May overlap call operations,
 * but not another shutdown/free. Does not reconnect or replay. */
zerobus_status_t zb_transport_shutdown(zb_transport_t *transport);
/* NULL-safe. Frees all remaining calls. Requires exclusive access to the
 * transport and its calls: stop/join all workers first. */
void zb_transport_free(zb_transport_t *transport);

#endif /* ZB_NETWORK_TRANSPORT_H */
