#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "error.h"
#include "headers_provider.h"
#include "internal/common/utils.h"
#include "internal/network/grpc_utils.h"
#include "internal/oauth/oauth.h"
#include "internal/platform/log.h"
#include "sdk.h" /* IWYU pragma: keep */
#include "stream.h"
#include "zerobus/common.h"
#include "zerobus/error.h"

struct zerobus_headers_provider {
    atomic_size_t ref_cnt;
    zerobus_get_headers_fn get_headers;
    zerobus_invalidate_headers_fn invalidate;
    zerobus_destroy_user_data_fn destroy;
    void *user_data;
};

/* ---- headers ----------------------------------------------------------- */

zerobus_status_t zerobus_headers_add(zerobus_headers_t *headers,
                                     zerobus_string_view_t name,
                                     zerobus_string_view_t value,
                                     zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (headers == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "headers must not be NULL");
    }
    if (zb_is_empty(name)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "header name must not be empty");
    }
    /* Names are case-insensitive, and HTTP/2 sends them lowercase. */
    char *name_copy = zb_strdup_view(name);
    if (name_copy == NULL) {
        ZB_ERROR("header allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    for (size_t i = 0; i < name.len; ++i) {
        if (name_copy[i] >= 'A' && name_copy[i] <= 'Z') {
            name_copy[i] = (char)(name_copy[i] - 'A' + 'a');
        }
    }
    if (!zb_is_valid_grpc_metadata((zerobus_string_view_t){name_copy, name.len},
                                   value)) {
        free(name_copy);
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "header must be a gRPC metadata name with a "
                       "printable-ASCII value");
    }
    if (strcmp(name_copy, "x-databricks-zerobus-table-name") == 0 ||
        strcmp(name_copy, "user-agent") == 0) {
        ZB_WARN("ignoring the %s header, which the SDK sets itself", name_copy);
        free(name_copy);
        return ZEROBUS_STATUS_OK;
    }
    char *value_copy =
        value.data != NULL ? zb_strdup_view(value) : zb_strdup("");
    if (value_copy == NULL) {
        ZB_ERROR("header allocation failed");
        free(name_copy);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    return zb_headers_add_internal(headers, name_copy, value_copy);
}

zerobus_status_t zb_headers_add_internal(zerobus_headers_t *headers, char *name,
                                         char *value)
{
    if (name == NULL || value == NULL) {
        free(name);
        zb_secure_free_cstr(value);
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (headers->count == headers->capacity) {
        size_t capacity = headers->capacity ? 2 * headers->capacity : 4;
        zb_header_t *items =
            realloc(headers->items, capacity * sizeof(*headers->items));
        if (items == NULL) {
            ZB_ERROR("header set allocation failed");
            free(name);
            zb_secure_free_cstr(value);
            return ZEROBUS_STATUS_OUT_OF_MEMORY;
        }
        headers->items = items;
        headers->capacity = capacity;
    }
    headers->items[headers->count++] = (zb_header_t){name, value};
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_headers_set_error_message(zerobus_headers_t *headers,
                                  zerobus_string_view_t message,
                                  zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (headers == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "headers must not be NULL");
    }
    if (!zb_is_valid_string(message)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "error message must be non-empty valid UTF-8");
    }
    char *copy = zb_strdup_view(message);
    if (copy == NULL) {
        ZB_ERROR("header error message allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    free(headers->error_message);
    headers->error_message = copy;
    return ZEROBUS_STATUS_OK;
}

void zb_headers_clear(zerobus_headers_t *headers)
{
    for (size_t i = 0; i < headers->count; ++i) {
        free(headers->items[i].name);
        zb_secure_free_cstr(headers->items[i].value);
    }
    free(headers->items);
    free(headers->error_message);
    *headers = (zerobus_headers_t){0};
}

/* ---- provider ---------------------------------------------------------- */

zerobus_status_t zerobus_headers_provider_new(
    zerobus_get_headers_fn get_headers, zerobus_destroy_user_data_fn destroy,
    void *user_data, zerobus_headers_provider_t **out_provider,
    zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (get_headers == NULL || out_provider == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "get_headers and out_provider must not be NULL");
    }
    zerobus_headers_provider_t *provider =
        (zerobus_headers_provider_t *)calloc(1, sizeof(*provider));
    if (provider == NULL) {
        ZB_ERROR("headers provider allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    atomic_init(&provider->ref_cnt, 1);
    provider->get_headers = get_headers;
    provider->destroy = destroy;
    provider->user_data = user_data;
    *out_provider = provider;
    ZB_DEBUG("headers provider created");
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_headers_provider_set_invalidate(
    zerobus_headers_provider_t *provider,
    zerobus_invalidate_headers_fn invalidate, zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (provider == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "provider must not be NULL");
    }
    if (atomic_load(&provider->ref_cnt) != 1) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "provider is already in use by a builder or stream");
    }
    provider->invalidate = invalidate;
    return ZEROBUS_STATUS_OK;
}

void zb_headers_provider_ref(zerobus_headers_provider_t *provider)
{
    atomic_fetch_add(&provider->ref_cnt, 1);
}

zerobus_status_t zb_headers_provider_get(zerobus_headers_provider_t *provider,
                                         zerobus_headers_t *headers,
                                         zerobus_error_t **out_error)
{
    zerobus_status_t status =
        provider->get_headers(provider->user_data, headers);
    if (status != ZEROBUS_STATUS_OK) {
        if (headers->error_message != NULL) {
            zb_fail(out_error, status, "headers provider failed: %s",
                    headers->error_message);
        } else {
            zb_fail(out_error, status, "headers provider failed");
        }
        zb_headers_clear(headers);
    }
    return status;
}

void zb_headers_provider_invalidate(zerobus_headers_provider_t *provider)
{
    if (provider->invalidate != NULL) {
        provider->invalidate(provider->user_data);
    }
}

void zerobus_headers_provider_free(zerobus_headers_provider_t *provider)
{
    if (provider == NULL) {
        return;
    }
    if (atomic_fetch_sub(&provider->ref_cnt, 1) != 1) {
        return;
    }
    if (provider->destroy != NULL) {
        provider->destroy(provider->user_data);
    }
    free(provider);
    ZB_DEBUG("headers provider freed");
}

/* ---- static provider --------------------------------------------------- */

/* user_data is the provider's own copy of the headers. */
static zerobus_status_t ZEROBUS_CALL
static_get_headers(void *user_data, zerobus_headers_t *headers)
{
    const zerobus_headers_t *copy = (const zerobus_headers_t *)user_data;
    for (size_t i = 0; i < copy->count; ++i) {
        zerobus_status_t status = zerobus_headers_add(
            headers, zerobus_string_view(copy->items[i].name),
            zerobus_string_view(copy->items[i].value), NULL);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
    }
    return ZEROBUS_STATUS_OK;
}

static void ZEROBUS_CALL static_destroy(void *user_data)
{
    zb_headers_clear((zerobus_headers_t *)user_data);
    free(user_data);
}

zerobus_status_t zerobus_static_headers_provider_new(
    const zerobus_string_view_t *names, const zerobus_string_view_t *values,
    size_t count, zerobus_headers_provider_t **out_provider,
    zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if ((count != 0 && (names == NULL || values == NULL)) ||
        out_provider == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "names and values (unless count is 0) and "
                       "out_provider must not be NULL");
    }
    zerobus_headers_t *copy = (zerobus_headers_t *)calloc(1, sizeof(*copy));
    if (copy == NULL) {
        ZB_ERROR("static headers allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    for (size_t i = 0; i < count && status == ZEROBUS_STATUS_OK; ++i) {
        status = zerobus_headers_add(copy, names[i], values[i], out_error);
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = zerobus_headers_provider_new(
            static_get_headers, static_destroy, copy, out_provider, out_error);
    }
    if (status != ZEROBUS_STATUS_OK) {
        static_destroy(copy);
    }
    return status;
}

/* ---- oauth provider ---------------------------------------------------- */

struct oauth_state {
    const zerobus_stream_t *stream; /* borrowed */
    char *client_id;
    char *client_secret; /* zeroed on free */
};

static zerobus_status_t ZEROBUS_CALL
oauth_get_headers(void *user_data, zerobus_headers_t *headers)
{
    const struct oauth_state *state = (const struct oauth_state *)user_data;
    const zerobus_stream_t *stream = state->stream;
    const zerobus_sdk_t *sdk = stream->sdk;
    zb_fetched_token_t token = {0};
    zerobus_error_t *error = NULL;
    zerobus_status_t status = zb_oauth_fetch_token(
        sdk->uc_tls_context, sdk->uc_endpoint, sdk->workspace_id, stream->table,
        state->client_id, state->client_secret,
        zb_deadline_after_ms(stream->config.recovery_timeout_ms), &token,
        &error);
    if (status != ZEROBUS_STATUS_OK) {
        zerobus_string_view_t message = {0};
        if (error != NULL) {
            message = zerobus_error_message(error);
        }
        ZB_ERROR("OAuth token fetch failed%s%.*s", message.len != 0 ? ": " : "",
                 (int)message.len, message.data);
        zerobus_error_free(error);
        return status;
    }

    static const char prefix[] = "Bearer ";
    const size_t prefix_len = sizeof(prefix) - 1;
    size_t token_len = strlen(token.token);
    size_t len = prefix_len + token_len;
    char *value = (char *)malloc(len);
    if (value == NULL) {
        ZB_ERROR("authorization header allocation failed");
        zb_fetched_token_clear(&token);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    memcpy(value, prefix, prefix_len);
    memcpy(value + prefix_len, token.token, token_len);
    zb_fetched_token_clear(&token);
    status = zerobus_headers_add(headers, zerobus_string_view("authorization"),
                                 (zerobus_string_view_t){value, len}, NULL);
    zb_secure_free(value, len);
    return status;
}

static void ZEROBUS_CALL oauth_destroy(void *user_data)
{
    struct oauth_state *state = (struct oauth_state *)user_data;
    free(state->client_id);
    zb_secure_free_cstr(state->client_secret);
    free(state);
}

zerobus_status_t
zb_oauth_headers_provider_new(const zerobus_stream_t *stream,
                              const char *client_id, const char *client_secret,
                              zerobus_headers_provider_t **out_provider)
{
    struct oauth_state *state = (struct oauth_state *)calloc(1, sizeof(*state));
    if (state == NULL) {
        ZB_ERROR("OAuth provider allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    state->stream = stream;
    state->client_id = zb_strdup(client_id);
    state->client_secret = zb_strdup(client_secret);
    if (state->client_id == NULL || state->client_secret == NULL) {
        ZB_ERROR("OAuth provider configuration allocation failed");
        oauth_destroy(state);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status = zerobus_headers_provider_new(
        oauth_get_headers, oauth_destroy, state, out_provider, NULL);
    if (status != ZEROBUS_STATUS_OK) {
        oauth_destroy(state);
    }
    return status;
}
