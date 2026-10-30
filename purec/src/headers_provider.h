#ifndef ZB_HEADERS_PROVIDER_H
#define ZB_HEADERS_PROVIDER_H

#include "zerobus/headers_provider.h"

typedef struct {
    char *name;
    char *value; /* May be a credential; zeroed on free. */
} zb_header_t;

/* A header set. Zero-initialized storage is an empty set. */
struct zerobus_headers {
    zb_header_t *items;
    size_t count;
    size_t capacity;
    char *error_message; /* Set by a failing get_headers, or NULL. */
};

/* On failure, frees both name and value. */
zerobus_status_t zb_headers_add_internal(zerobus_headers_t *headers, char *name,
                                         char *value);

/* Frees every header, zeroing the values, and the error message, and leaves an
 * empty set. */
void zb_headers_clear(zerobus_headers_t *headers);

/* Caller must hold a live, non-NULL provider reference; release with
 * zerobus_headers_provider_free. */
void zb_headers_provider_ref(zerobus_headers_provider_t *provider);

/* Fills headers, which must be empty. On failure, headers is left empty, and
 * the error carries the provider's message, if it set one. */
zerobus_status_t zb_headers_provider_get(zerobus_headers_provider_t *provider,
                                         zerobus_headers_t *headers,
                                         zerobus_error_t **out_error);

/* Tells the provider that the server rejected the credentials from its last
 * get. */
void zb_headers_provider_invalidate(zerobus_headers_provider_t *provider);

/*
 * Creates a provider for stream that supplies an "authorization: Bearer
 * <token>" header, where the token is a Unity Catalog OAuth token for the
 * stream's table, fetched with client_id and client_secret. stream is
 * borrowed and must outlive the provider. The credentials must already be
 * valid, as the stream builder setters ensure. They are copied, and
 * client_secret is zeroed on free. Fails only with OUT_OF_MEMORY.
 *
 * get fetches a new token on every call, within the stream's recovery
 * timeout.
 */
zerobus_status_t
zb_oauth_headers_provider_new(const zerobus_stream_t *stream,
                              const char *client_id, const char *client_secret,
                              zerobus_headers_provider_t **out_provider);

#endif /* ZB_HEADERS_PROVIDER_H */
