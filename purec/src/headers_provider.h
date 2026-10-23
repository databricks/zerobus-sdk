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
};

/* Frees every header, zeroing the values, and leaves an empty set. */
void zb_headers_clear(zerobus_headers_t *headers);

/* Caller must hold a live, non-NULL provider reference; release with
 * zerobus_headers_provider_free. */
void zb_headers_provider_ref(zerobus_headers_provider_t *provider);

/* Fills headers, which must be empty. On failure, headers is left empty. */
zerobus_status_t zb_headers_provider_get(zerobus_headers_provider_t *provider,
                                         zerobus_headers_t *headers);

/* Tells the provider that the server rejected the credentials from its last
 * get. */
void zb_headers_provider_invalidate(zerobus_headers_provider_t *provider);

#endif /* ZB_HEADERS_PROVIDER_H */
