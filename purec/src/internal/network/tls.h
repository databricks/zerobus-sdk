/* TLS client sessions over borrowed nonblocking platform sockets. */
#ifndef ZB_NETWORK_TLS_H
#define ZB_NETWORK_TLS_H

#include "internal/platform/network.h"

typedef struct zb_tls_ctx zb_tls_ctx_t;
typedef struct zb_tls zb_tls_t;

/* Creates the settings that sessions share: TLS 1.2 or newer and verified
 * server certificates. ca_file holds the trusted CA certificates. Uses the
 * system roots on NULL. Blocks while it reads them from disk. */
zerobus_status_t zb_tls_ctx_new(const char *ca_file, zb_tls_ctx_t **out_ctx);

/* Frees the context. Sessions created from it remain usable. */
void zb_tls_ctx_free(zb_tls_ctx_t *ctx);

/* Creates a session on socket without any I/O. Connect the socket before the
 * first handshake.
 * The socket is borrowed, never closed.
 * host is the DNS name or IP address (without brackets) the certificate must
 * match.
 * With http2, the server must select HTTP/2; without it, no protocol is
 * negotiated. */
zerobus_status_t zb_tls_new(const zb_tls_ctx_t *ctx, zb_socket_t socket,
                            const char *host, bool http2, zb_tls_t **out_tls);
void zb_tls_free(zb_tls_t *tls);

/* Advances the handshake without blocking. Call it until *out_ready is true,
 * waiting for *out_want on the socket between calls. Any status other than OK
 * means the handshake failed. */
zerobus_status_t zb_tls_handshake(zb_tls_t *tls, unsigned *out_want,
                                  bool *out_ready);

/* Reads decrypted data, at most one TLS record per call. If out->want is set,
 * wait for it and call again. */
zerobus_status_t zb_tls_read(zb_tls_t *tls, uint8_t *data, size_t len,
                             zb_io_result_t *out);

/* Writes data and sets out->bytes to how much was sent. If out->want is set
 * instead, wait for it and call again with the same data and len. */
zerobus_status_t zb_tls_write(zb_tls_t *tls, const uint8_t *data, size_t len,
                              zb_io_result_t *out);

/* Returns true if the session has pending data to read. */
bool zb_tls_pending(const zb_tls_t *tls);

#endif /* ZB_NETWORK_TLS_H */
