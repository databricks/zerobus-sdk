/* A client connection over TCP, plaintext or TLS: connecting with bounded
 * waits, and reads and writes that work the same either way. */
#ifndef ZB_NETWORK_CONNECTION_H
#define ZB_NETWORK_CONNECTION_H

#include "internal/network/tls.h"

typedef struct {
    zb_socket_t socket;
    zb_tls_t *tls; /* NULL for plaintext. */
} zb_connection_t;

/* A closed connection. */
#define ZB_CONNECTION_CLOSED ((zb_connection_t){ZB_SOCKET_INVALID, NULL})

/* Waits until the connection's socket is ready for want. Returns CANCELLED
 * once wakeup is signaled, even if the socket is ready too, or
 * DEADLINE_EXCEEDED if deadline passes before the socket is ready. The wakeup
 * signal is not drained. */
zerobus_status_t zb_connection_wait(const zb_connection_t *connection,
                                    unsigned want, zb_wakeup_t *wakeup,
                                    zb_deadline_t deadline);

/*
 * Connects *connection to the first address of host that accepts, then with tls
 * completes a TLS handshake that verifies the certificate for host. With
 * http2, the server must select HTTP/2. Waits as zb_connection_wait does,
 * except that the DNS lookup is not bounded: a deadline that passes during it
 * fails the open once it returns. On failure, *connection is closed.
 */
zerobus_status_t zb_connection_open(zb_connection_t *connection,
                                    const zb_tls_ctx_t *tls, const char *host,
                                    uint16_t port, bool http2,
                                    zb_wakeup_t *wakeup,
                                    zb_deadline_t deadline);

zerobus_status_t zb_connection_read(zb_connection_t *connection, uint8_t *data,
                                    size_t len, zb_io_result_t *out);

zerobus_status_t zb_connection_write(zb_connection_t *connection,
                                     const uint8_t *data, size_t len,
                                     zb_io_result_t *out);

/* Returns true if TLS has decrypted data waiting to be read. Always false for
 * plaintext. */
bool zb_connection_pending(const zb_connection_t *connection);

/* Closes the connection, which may already be closed. */
void zb_connection_close(zb_connection_t *connection);

#endif /* ZB_NETWORK_CONNECTION_H */
