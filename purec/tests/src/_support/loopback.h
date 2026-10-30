/* Helpers for test servers on a loopback port: the listener, a self-signed
 * certificate, and a TLS server context that uses it. */
#ifndef ZB_TEST_LOOPBACK_H
#define ZB_TEST_LOOPBACK_H

#include <openssl/ssl.h>
#include <stdbool.h>
#include <stdint.h>

/* Listens on an ephemeral loopback port. Returns the socket, or -1. The kernel
 * completes a few connections in the backlog, so tests need not accept them. */
int loopback_listen(uint16_t *out_port);

/* Writes a new self-signed certificate for localhost and 127.0.0.1, and its
 * key. */
bool loopback_write_certificate(const char *cert_file, const char *key_file);

/* A server context with the certificate and key, or NULL. With h2 it selects
 * HTTP/2 when the client offers it, otherwise no protocol. */
SSL_CTX *loopback_server_tls(const char *cert_file, const char *key_file,
                             bool h2);

#endif /* ZB_TEST_LOOPBACK_H */
