/* A gRPC connection that many calls share, run by an I/O thread of its own. */
#ifndef ZB_NETWORK_TRANSPORT_H
#define ZB_NETWORK_TRANSPORT_H

#include "channel.h"
#include "internal/platform/concurrency.h"
#include "tls.h"

typedef struct zb_transport zb_transport_t;
typedef struct zb_transport_call zb_transport_call_t;

typedef struct {
    const char *host;
    uint16_t port;
    bool plaintext;
    /* Required for TLS, NULL for plaintext. Borrowed until open returns. */
    const zb_tls_ctx_t *tls_context;
    size_t max_message_bytes;
    size_t max_header_bytes;
    /* Each call's send and receive queue sizes. */
    size_t send_queue_messages;
    size_t receive_queue_messages;
} zb_transport_config_t;

/* Starts the connection's I/O thread and waits until deadline for it to
 * connect. Opens no call. config is borrowed only for this call. A DNS lookup
 * cannot be interrupted, so a timeout may wait for it to finish. */
zerobus_status_t zb_transport_open(const zb_transport_config_t *config,
                                   zb_deadline_t deadline,
                                   zb_transport_t **out_transport);

/* Opens a call on path with headers, both borrowed only for this call.
 * Returns without waiting for the server, whose answer arrives on the call's
 * RX channel. May run concurrently with other calls' operations and with
 * shutdown. */
zerobus_status_t
zb_transport_call_open(zb_transport_t *transport, zerobus_string_view_t path,
                       const zb_metadata_t *headers, size_t header_count,
                       zb_deadline_t deadline, zb_transport_call_t **out_call);

/* The call's send channel, or NULL for a NULL call. End sending with
 * zb_transport_half_close, not by closing the channel. */
zb_channel_t *zb_transport_tx(const zb_transport_call_t *call);

/* The call's receive channel, or NULL for a NULL call. It ends, with OK and a
 * NULL message, once the call has ended. */
zb_channel_t *zb_transport_rx(const zb_transport_call_t *call);

/* The call's result, once RX has ended. NULL for a NULL call. */
const zb_rpc_result_t *zb_transport_get_result(const zb_transport_call_t *call);

/* Sends the queued messages, then ends sending, while receiving continues. */
zerobus_status_t zb_transport_half_close(zb_transport_call_t *call);

/* Ends the call as CANCELLED and wakes its waiting workers, while other calls
 * continue. Once it returns, the I/O thread no longer uses the call. Accepts
 * already ended calls. */
zerobus_status_t zb_transport_cancel(zb_transport_call_t *call);

/* Cancels the call and frees it with its queued messages. No other thread may
 * use the call by then. Accepts NULL. */
void zb_transport_call_free(zb_transport_call_t *call);

/* Stops the I/O thread and closes the connection, ending open calls as
 * CANCELLED. Their received messages and results stay readable until they are
 * freed. May overlap call operations, but not another shutdown or free. */
zerobus_status_t zb_transport_shutdown(zb_transport_t *transport);

/* Shuts down if needed, then frees the transport and its remaining calls. No
 * other thread may use them by then. Accepts NULL. */
void zb_transport_free(zb_transport_t *transport);

#endif /* ZB_NETWORK_TRANSPORT_H */
