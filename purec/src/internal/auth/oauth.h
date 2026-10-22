/* Unity Catalog OAuth client credentials: mints table-scoped Zerobus tokens
 * and caches each one until shortly before it expires. */
#ifndef ZB_AUTH_OAUTH_H
#define ZB_AUTH_OAUTH_H

#include "internal/platform/concurrency.h"
#include "zerobus/common.h"

typedef struct zb_oauth zb_oauth_t;

/* Copies its inputs. uc_endpoint is a validated http(s)://host[:port] origin
 * and table a catalog.schema.table name. */
zerobus_status_t zb_oauth_new(const char *uc_endpoint, const char *workspace_id,
                              zerobus_string_view_t table,
                              zerobus_string_view_t client_id,
                              zerobus_string_view_t client_secret,
                              zb_oauth_t **out_oauth);

/* Sets *out_token to a malloc-owned copy of a valid token, minting a new one
 * when the cached token is missing or about to expire. Concurrent callers
 * share one mint. Free the copy with zb_secure_free_cstr. */
zerobus_status_t zb_oauth_token(zb_oauth_t *oauth, zb_deadline_t deadline,
                                char **out_token, zerobus_error_t **out_error);

/* Drops the cached token, so the next call mints. */
void zb_oauth_invalidate(zb_oauth_t *oauth);

/* NULL-safe. Zeroes the secret and token. */
void zb_oauth_free(zb_oauth_t *oauth);

/* Finds a top-level member of the JSON object text: a string value is
 * unescaped into a malloc-owned *out_string, a number or numeric string is
 * stored in *out_number. Nested values are skipped. False if the text is
 * malformed or the member is missing or of another type. */
bool zb_json_get_string(const char *text, size_t len, const char *key,
                        char **out_string);
bool zb_json_get_u64(const char *text, size_t len, const char *key,
                     uint64_t *out_number);

#endif /* ZB_AUTH_OAUTH_H */
