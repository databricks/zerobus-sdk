/* The I/O owner: a thread that opens a connection, then moves bytes between
 * its socket and the gRPC session until it stops. */
#ifndef ZB_NETWORK_IO_H
#define ZB_NETWORK_IO_H

#include "connection.h"
#include "grpc.h"

#define ZB_IO_CHUNK 16384u

typedef struct {
    /* Used only by the runner once it starts. */
    zb_grpc_t grpc;
    zb_connection_t connection;
    const zb_tls_ctx_t *tls_context; /* Borrowed until the connection opens. */
    char *host;                      /* Owned copy, NUL-terminated. */
    uint16_t port;
    /* Received bytes. While paused, gRPC keeps a pointer into them. */
    uint8_t input[ZB_IO_CHUNK];
    /* Wakes the runner to call service. Before on_ready, a signal stops the
     * runner instead. Lives until zb_io_destroy, since callers borrow it. */
    zb_wakeup_t *wakeup;
    /* Stops the runner as DEADLINE_EXCEEDED if it is still waiting to connect
     * then. The DNS lookup is not bounded, but the deadline is checked once it
     * returns. */
    zb_deadline_t connect_deadline;

    /* Set before start, all required. The runner passes context to each
     * callback. */
    void *context;
    /* Runs between iterations of the loop. May use the session, but must not
     * re-enter the loop. A status other than OK stops the runner. */
    zerobus_status_t (*service)(void *context);
    /* Runs once the connection is open, before any response arrives. */
    void (*on_ready)(void *context);
    /* Runs last, after the session and the connection are closed, with the
     * status that stopped the runner. */
    void (*on_stopped)(void *context, zerobus_status_t status);
} zb_io_t;

/* Prepares io without network I/O. tls_context is required exactly when
 * config->tls is set, and is borrowed until the runner calls on_ready or
 * on_stopped. Everything else is borrowed only for this call. host must be
 * non-empty without NUL bytes, and port nonzero. The runner stops connecting if
 * it is still waiting at connect_deadline. On failure, io is left destroyed. */
zerobus_status_t zb_io_init(zb_io_t *io, const zb_grpc_config_t *config,
                            zerobus_string_view_t host, uint16_t port,
                            const zb_tls_ctx_t *tls_context,
                            zb_deadline_t connect_deadline);

/* Frees what io holds, also after a failed init. Call once the runner, if
 * started, is joined and nothing uses its wakeup. */
void zb_io_destroy(zb_io_t *io);

/* The runner's thread entry, with arg an initialized io. Connects to host,
 * completes the TLS handshake if tls_context is set, then drives the gRPC
 * session. Returns NULL after on_stopped. */
void *zb_io_run(void *arg);

#endif /* ZB_NETWORK_IO_H */
