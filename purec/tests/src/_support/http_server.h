/* A scripted HTTP server on a loopback port, shared by the tests that make
 * HTTP requests. */
#ifndef ZB_TEST_HTTP_SERVER_H
#define ZB_TEST_HTTP_SERVER_H

#include <openssl/ssl.h>
#include <stdbool.h>
#include <stdint.h>

#include "internal/platform/concurrency.h"

enum { HTTP_SERVER_MAX_PIECES = 4 };

/*
 * Serves connections one at a time until stopped. On each, it reads one
 * request, sends the reply pieces a moment apart, and closes, or with hold
 * first waits for the client to close. Set tls, reply and hold before start.
 */
typedef struct {
    SSL_CTX *tls; /* NULL serves plain HTTP. */
    const char *reply[HTTP_SERVER_MAX_PIECES];
    bool hold;
    uint16_t port;
    size_t served;      /* Connections accepted. */
    char request[8192]; /* The last request. */
    size_t request_len;
    int listener;
    zb_thread_t thread;
} http_server_t;

bool http_server_start(http_server_t *server);

/* Stops serving and waits for the server. Read the fields only after this. */
void http_server_stop(http_server_t *server);

#endif /* ZB_TEST_HTTP_SERVER_H */
