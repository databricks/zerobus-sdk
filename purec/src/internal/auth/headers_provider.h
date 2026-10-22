/* Internal side of zerobus_headers_provider_t: resolving the headers a stream
 * sends when it opens. */
#ifndef ZB_AUTH_HEADERS_PROVIDER_H
#define ZB_AUTH_HEADERS_PROVIDER_H

#include "internal/network/message.h"
#include "internal/platform/concurrency.h"
#include "zerobus/headers.h"

/* Owned header copies, as resolved for one open attempt. */
typedef struct {
    zb_metadata_t *items;
    size_t count;
} zb_headers_t;

/* An OAuth client-credentials provider for one table; see auth/oauth.h. */
zerobus_status_t
zb_oauth_headers_provider_new(const char *uc_endpoint, const char *workspace_id,
                              zerobus_string_view_t table,
                              zerobus_string_view_t client_id,
                              zerobus_string_view_t client_secret,
                              zerobus_headers_provider_t **out_provider);

/* Adds a reference; release it with zerobus_headers_provider_free. */
zerobus_headers_provider_t *
zb_headers_provider_ref(zerobus_headers_provider_t *provider);

/* Resolves the provider's headers for table into *out, which the caller
 * clears. A custom provider runs on the calling thread and is not bounded by
 * deadline. */
zerobus_status_t zb_headers_provider_get(zerobus_headers_provider_t *provider,
                                         zerobus_string_view_t table,
                                         zb_deadline_t deadline,
                                         zb_headers_t *out,
                                         zerobus_error_t **out_error);

/* Reports that the server rejected the provider's credentials for table. */
void zb_headers_provider_invalidate(zerobus_headers_provider_t *provider,
                                    zerobus_string_view_t table);

/* Frees the copies, zeroing them, and resets *headers. */
void zb_headers_clear(zb_headers_t *headers);

#endif /* ZB_AUTH_HEADERS_PROVIDER_H */
