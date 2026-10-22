/* Blocking HTTP/1.1 requests on a fresh connection, for OAuth token requests.
 * Not used on the ingestion path. */
#ifndef ZB_AUTH_HTTP_H
#define ZB_AUTH_HTTP_H

#include "internal/network/tls.h"

/* Largest response body read, in bytes. */
#define ZB_HTTP_MAX_RESPONSE (1024u * 1024u)

typedef struct {
    uint32_t status; /* HTTP status code. */
    char *body;      /* malloc-owned and NUL-terminated. */
    size_t body_len;
} zb_http_response_t;

/* Sends one POST of body to path on host:port and reads the whole response,
 * all before deadline. tls selects verified TLS, without ALPN; NULL means
 * plaintext. headers holds extra request header lines, each ending in "\r\n",
 * and may be "". Any HTTP status returns OK with *out filled in; a connection,
 * TLS or framing failure returns UNAVAILABLE, a response over
 * ZB_HTTP_MAX_RESPONSE returns RESOURCE_EXHAUSTED. Release *out with
 * zb_http_response_clear. */
zerobus_status_t zb_http_post(const zb_tls_ctx_t *tls, const char *host,
                              uint16_t port, const char *path,
                              const char *headers, const char *content_type,
                              const uint8_t *body, size_t body_len,
                              zb_deadline_t deadline, zb_http_response_t *out);

void zb_http_response_clear(zb_http_response_t *response);

#endif /* ZB_AUTH_HTTP_H */
