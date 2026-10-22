/* Connection setup and byte-driving loop. No call or worker-channel state. */
#ifndef ZB_NETWORK_IO_H
#define ZB_NETWORK_IO_H

#include "grpc.h"
#include "tls.h"

#define ZB_IO_CHUNK 16384u
/* Outgoing frames are coalesced up to this many bytes per socket write. */
#define ZB_IO_OUTPUT 65536u

typedef struct zb_io zb_io_t;

struct zb_io {
    /* Prepared before start, then used exclusively by the runner. Input
     * outlives every view retained by gRPC/nghttp2. */
    zb_grpc_t grpc;
    zb_net_addresses_t *addresses;
    zb_socket_t socket;
    const zb_tls_ctx_t *tls_context; /* Borrowed until session creation. */
    char *tls_host;
    zb_tls_t *tls;
    zb_deadline_t deadline;
    uint8_t input[ZB_IO_CHUNK];
    /* Frames copied out of nghttp2, written from output_start. */
    uint8_t output[ZB_IO_OUTPUT];
    size_t output_start;
    size_t output_end;
    /* Borrowed by worker channels; kept alive after runner exit. */
    zb_wakeup_t *wakeup;

    /* Set before start, invoked only by the runner. service runs between
     * byte-driving turns and during setup; return non-OK to stop. It may
     * operate on the session, but must not re-enter the loop. prepare_wait
     * runs before the loop sleeps: it arranges for the next worker change to
     * signal the wakeup, and returns true if work the loop can do now is
     * already waiting, in which case the loop does not sleep. can_send is
     * false while output waits for the socket, so queued messages are not
     * yet work. on_stopped runs after gRPC and network teardown, while
     * wakeup is still valid. */
    void *context;
    zerobus_status_t (*service)(void *context);
    bool (*prepare_wait)(void *context, bool can_send);
    void (*on_ready)(void *context);
    void (*on_stopped)(void *context, zerobus_status_t status);
};

/* Prepares the session and DNS addresses on the creating thread. tls_context
 * must be non-NULL exactly when config->tls is true. It is borrowed through
 * runner setup, until on_ready/on_stopped; other inputs are borrowed only
 * during init. The TLS session retains the native context independently.
 * Failure cleans up io without freeing the supplied context. */
zerobus_status_t zb_io_init(zb_io_t *io, const zb_grpc_config_t *config,
                            const char *host, uint16_t port,
                            const zb_tls_ctx_t *tls_context,
                            zb_deadline_t deadline);
/* Exclusive access, after joining the runner and freeing all channels that
 * borrow its wakeup. Also accepts failed or never-started initialization. */
void zb_io_destroy(zb_io_t *io);
void *zb_io_run(void *arg);

#endif /* ZB_NETWORK_IO_H */
