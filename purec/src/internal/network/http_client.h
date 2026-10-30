/* One-shot HTTP/1.1 requests, each on a fresh connection, for Unity Catalog
 * calls such as OAuth token requests. */
#ifndef ZB_NETWORK_HTTP_CLIENT_H
#define ZB_NETWORK_HTTP_CLIENT_H

#include "internal/network/tls.h"

/* Largest response body accepted, in bytes. */
#define ZB_HTTP_CLIENT_MAX_BODY (1024u * 1024u)

typedef struct {
    uint32_t status;
    char *body;
    size_t body_len;
} zb_http_response_t;

/* POSTs body, a form (application/x-www-form-urlencoded), to path on origin
 * and reads the whole response by deadline, except for the DNS lookup, which
 * is unbounded. origin is an
 * "http(s)://host[:port]" that zb_url_validate accepts. https needs tls, and
 * header names must not contain ':'.
 *
 * Any HTTP status returns OK and fills *out, for zb_http_response_clear to
 * release. Otherwise returns UNAVAILABLE for a failed or cut-short connection,
 * UNAUTHENTICATED for an untrusted certificate, INTERNAL for a malformed
 * response, UNIMPLEMENTED for Transfer-Encoding, and OUT_OF_RANGE past the
 * size limits. */
zerobus_status_t zb_http_client_post(const zb_tls_ctx_t *tls,
                                     const char *origin, const char *path,
                                     const zerobus_headers_t *headers,
                                     const char *body, size_t body_len,
                                     zb_deadline_t deadline,
                                     zb_http_response_t *out);

void zb_http_response_clear(zb_http_response_t *response);

#endif /* ZB_NETWORK_HTTP_CLIENT_H */
