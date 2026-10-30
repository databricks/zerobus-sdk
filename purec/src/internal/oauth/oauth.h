/* Unity Catalog OAuth tokens for Zerobus, fetched with client credentials. */
#ifndef ZB_OAUTH_H
#define ZB_OAUTH_H

#include <stdint.h>

#include "internal/network/tls.h"
#include "zerobus/common.h"

typedef struct {
    char *token;         /* Zeroed on free. */
    uint64_t expires_in; /* Lifetime in seconds; 0 if the response had none. */
} zb_fetched_token_t;

/* Frees the token, zeroing it, and leaves the struct empty. */
void zb_fetched_token_clear(zb_fetched_token_t *token);

/*
 * Fetches a token that lets client_id write to table in workspace_id, from the
 * Unity Catalog at uc_endpoint. The arguments must already be valid. An https
 * uc_endpoint needs tls, and deadline bounds everything but the DNS lookup.
 *
 * Returns OK and fills *out_token. Otherwise leaves *out_token untouched and
 * returns UNAVAILABLE for a 5xx response, UNAUTHENTICATED for any other
 * non-2xx response, INTERNAL for a 2xx response without an access_token that
 * is a non-empty header value, or the failure of zb_http_client_post.
 */
zerobus_status_t
zb_oauth_fetch_token(const zb_tls_ctx_t *tls, const char *uc_endpoint,
                     const char *workspace_id, const char *table,
                     const char *client_id, const char *client_secret,
                     zb_deadline_t deadline, zb_fetched_token_t *out_token,
                     zerobus_error_t **out_error);

#endif /* ZB_OAUTH_H */
