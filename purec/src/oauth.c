#include <stdlib.h>
#include <time.h>

#include "error.h"
#include "internal/common/utils.h"
#include "internal/network/tls.h"
#include "internal/oauth/oauth.h"
#include "internal/platform/concurrency.h"
#include "internal/platform/log.h"
#include "sdk.h"
#include "zerobus/oauth.h"

/* The limit a stream's token request has under the default recovery
 * timeout. */
#define TOKEN_TIMEOUT_MS 15000u

static uint64_t unix_time_ms(void)
{
    struct timespec now = {0};
    (void)timespec_get(&now, TIME_UTC);
    return (uint64_t)now.tv_sec * 1000u + (uint64_t)now.tv_nsec / 1000000u;
}

/* 0 for no lifetime, and UINT64_MAX for one past the range. */
static uint64_t expires_at_ms(uint64_t requested_at_ms, uint64_t expires_in)
{
    if (expires_in == 0) {
        return 0;
    }
    if (expires_in > (UINT64_MAX - requested_at_ms) / 1000u) {
        return UINT64_MAX;
    }
    return requested_at_ms + expires_in * 1000u;
}

zerobus_status_t zerobus_get_token(zerobus_string_view_t workspace_url,
                                   zerobus_string_view_t table_name,
                                   zerobus_string_view_t client_id,
                                   zerobus_string_view_t client_secret,
                                   zerobus_string_view_t workspace_id,
                                   zerobus_oauth_token_t *out_token,
                                   zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (out_token == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "out_token must not be NULL");
    }
    if (!zb_is_valid_string(workspace_url)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "workspace URL must be non-empty valid UTF-8");
    }
    if (!zb_validate_endpoint(workspace_url, "workspace URL", out_error)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (!zb_is_valid_string(table_name)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "table name must be non-empty valid UTF-8");
    }
    if (!zb_table_name_is_valid(table_name)) {
        return zb_fail(
            out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
            "table name must be fully qualified as catalog.schema.table");
    }
    if (!zb_is_valid_string(client_id) || !zb_is_valid_string(client_secret)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "client id and secret must be non-empty valid UTF-8");
    }
    if (!zb_is_valid_string(workspace_id)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "workspace id must be non-empty valid UTF-8");
    }

    zb_tls_ctx_t *tls = NULL;
    zb_fetched_token_t token = {0};
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    char *url = zb_strdup_view(workspace_url);
    char *table = zb_strdup_view(table_name);
    char *id = zb_strdup_view(client_id);
    char *secret = zb_strdup_view(client_secret);
    char *workspace = zb_strdup_view(workspace_id);
    if (url == NULL || table == NULL || id == NULL || secret == NULL ||
        workspace == NULL) {
        ZB_ERROR("OAuth token request allocation failed");
        status = ZEROBUS_STATUS_OUT_OF_MEMORY;
        goto done;
    }
    if (zb_starts_with(url, workspace_url.len, "https://")) {
        status = zb_tls_ctx_new(NULL, &tls);
        if (status != ZEROBUS_STATUS_OK) {
            status = zb_fail(out_error, status, "setting up TLS failed");
            goto done;
        }
    }
    uint64_t requested_at_ms = unix_time_ms();
    status = zb_oauth_fetch_token(tls, url, workspace, table, id, secret,
                                  zb_deadline_after_ms(TOKEN_TIMEOUT_MS),
                                  &token, out_error);
    if (status == ZEROBUS_STATUS_OK) {
        out_token->access_token = token.token;
        out_token->expires_at_unix_ms =
            expires_at_ms(requested_at_ms, token.expires_in);
    }
done:
    zb_tls_ctx_free(tls);
    free(url);
    free(table);
    free(id);
    zb_secure_free_cstr(secret);
    free(workspace);
    return status;
}

void zerobus_oauth_token_free(zerobus_oauth_token_t *token)
{
    if (token == NULL) {
        return;
    }
    zb_secure_free_cstr(token->access_token);
    *token = (zerobus_oauth_token_t){0};
}
