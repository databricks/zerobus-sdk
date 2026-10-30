/* One-shot HTTP/1.1 requests, each on a fresh connection, for Unity Catalog
 * calls such as OAuth token requests. */
#ifndef ZB_NETWORK_HTTP_CLIENT_H
#define ZB_NETWORK_HTTP_CLIENT_H

#include "internal/network/tls.h"

/* Largest response body accepted, in bytes. */
#define ZB_HTTP_CLIENT_MAX_BODY (1024u * 1024u)

typedef struct {
    uint32_t status; /* The final status, never 1xx. */
    /* Owned, with a NUL after body_len bytes. The body itself may contain
     * NUL, so read body_len bytes. */
    char *body;
    size_t body_len;
} zb_http_response_t;

/* POSTs body, a form (application/x-www-form-urlencoded), to path on origin
 * and reads the whole response by deadline, except for the DNS lookup, which
 * is unbounded. origin is an "http(s)://host[:port]" that zb_url_validate
 * accepts. https needs tls. Header names must be HTTP tokens other than Host,
 * Content-Length, Transfer-Encoding and Connection, which the client writes
 * itself.
 *
 * Any HTTP status returns OK and fills *out, for zb_http_response_clear to
 * release. Otherwise returns UNAVAILABLE for a failed, untrusted or cut-short
 * connection (over TLS, a body that ends with the connection needs the
 * server's close_notify), DEADLINE_EXCEEDED once deadline passes, INTERNAL for
 * a malformed response, UNIMPLEMENTED for a Transfer-Encoding other than
 * chunked, OUT_OF_RANGE past the size limits, RESOURCE_EXHAUSTED when out of
 * sockets, and INVALID_ARGUMENT or OUT_OF_MEMORY. */
zerobus_status_t zb_http_client_post(const zb_tls_ctx_t *tls,
                                     const char *origin, const char *path,
                                     const zerobus_headers_t *headers,
                                     const char *body, size_t body_len,
                                     zb_deadline_t deadline,
                                     zb_http_response_t *out);

void zb_http_response_clear(zb_http_response_t *response);

#endif /* ZB_NETWORK_HTTP_CLIENT_H */
