#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "error.h"
#include "headers_provider.h"
#include "internal/common/utils.h"
#include "internal/platform/log.h"

struct zerobus_headers_provider {
    atomic_size_t ref_cnt;
    zerobus_headers_provider_vtable_t vtable;
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
    if (!zb_is_valid_string(name)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "header name must be non-empty valid UTF-8");
    }
    if (value.len != 0 && !zb_is_valid_string(value)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "header value must be valid UTF-8");
    }
    char *name_copy = zb_strdup_view(name);
    char *value_copy =
        value.data != NULL ? zb_strdup_view(value) : zb_strdup("");
    if (name_copy == NULL || value_copy == NULL) {
        ZB_ERROR("header allocation failed");
        free(name_copy);
        zb_secure_free_cstr(value_copy);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    if (headers->count == headers->capacity) {
        size_t capacity = headers->capacity ? 2 * headers->capacity : 4;
        zb_header_t *items =
            realloc(headers->items, capacity * sizeof(*headers->items));
        if (items == NULL) {
            ZB_ERROR("header set allocation failed");
            free(name_copy);
            zb_secure_free_cstr(value_copy);
            return ZEROBUS_STATUS_OUT_OF_MEMORY;
        }
        headers->items = items;
        headers->capacity = capacity;
    }
    headers->items[headers->count++] = (zb_header_t){name_copy, value_copy};
    return ZEROBUS_STATUS_OK;
}

void zb_headers_clear(zerobus_headers_t *headers)
{
    for (size_t i = 0; i < headers->count; ++i) {
        free(headers->items[i].name);
        zb_secure_free_cstr(headers->items[i].value);
    }
    free(headers->items);
    *headers = (zerobus_headers_t){0};
}

/* ---- provider ---------------------------------------------------------- */

zerobus_status_t zerobus_headers_provider_new(
    const zerobus_headers_provider_vtable_t *vtable, void *user_data,
    zerobus_headers_provider_t **out_provider, zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (vtable == NULL || out_provider == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "vtable and out_provider must not be NULL");
    }
    if (vtable->get_headers == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "get_headers must not be NULL");
    }
    zerobus_headers_provider_t *provider =
        (zerobus_headers_provider_t *)calloc(1, sizeof(*provider));
    if (provider == NULL) {
        ZB_ERROR("headers provider allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    atomic_init(&provider->ref_cnt, 1);
    provider->vtable = *vtable;
    provider->user_data = user_data;
    *out_provider = provider;
    ZB_DEBUG("headers provider created");
    return ZEROBUS_STATUS_OK;
}

void zb_headers_provider_ref(zerobus_headers_provider_t *provider)
{
    atomic_fetch_add(&provider->ref_cnt, 1);
}

zerobus_status_t zb_headers_provider_get(zerobus_headers_provider_t *provider,
                                         zerobus_headers_t *headers)
{
    zerobus_status_t status =
        provider->vtable.get_headers(provider->user_data, headers);
    if (status != ZEROBUS_STATUS_OK) {
        zb_headers_clear(headers);
    }
    return status;
}

void zb_headers_provider_invalidate(zerobus_headers_provider_t *provider)
{
    if (provider->vtable.invalidate != NULL) {
        provider->vtable.invalidate(provider->user_data);
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
    if (provider->vtable.destroy != NULL) {
        provider->vtable.destroy(provider->user_data);
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

static const zerobus_headers_provider_vtable_t static_vtable = {
    static_get_headers,
    NULL,
    static_destroy,
};

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
        status = zerobus_headers_provider_new(&static_vtable, copy,
                                              out_provider, out_error);
    }
    if (status != ZEROBUS_STATUS_OK) {
        static_destroy(copy);
    }
    return status;
}

/* ---- empty provider ---------------------------------------------------- */

zerobus_status_t
zb_empty_headers_provider_new(zerobus_headers_provider_t **out_provider)
{
    return zerobus_static_headers_provider_new(NULL, NULL, 0, out_provider,
                                               NULL);
}

/* ---- oauth provider ---------------------------------------------------- */

struct oauth_state {
    char *uc_endpoint;
    char *workspace_id;
    char *table;
    char *client_id;
    char *client_secret; /* zeroed on free */
};

static zerobus_status_t ZEROBUS_CALL
oauth_get_headers(void *user_data, zerobus_headers_t *headers)
{
    char *token = NULL;
    /* TODO: fetch the token for the provider's table from the Unity Catalog
     * endpoint. */
    (void)user_data;
    if (token == NULL) {
        return ZEROBUS_STATUS_UNIMPLEMENTED;
    }

    static const char prefix[] = "Bearer ";
    const size_t prefix_len = sizeof(prefix) - 1;
    size_t token_len = strlen(token);
    size_t len = prefix_len + token_len;
    char *value = (char *)malloc(len);
    if (value == NULL) {
        ZB_ERROR("authorization header allocation failed");
        zb_secure_free_cstr(token);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    memcpy(value, prefix, prefix_len);
    memcpy(value + prefix_len, token, token_len);
    zb_secure_free_cstr(token);
    zerobus_status_t status =
        zerobus_headers_add(headers, zerobus_string_view("authorization"),
                            (zerobus_string_view_t){value, len}, NULL);
    zb_secure_free(value, len);
    return status;
}

static void ZEROBUS_CALL oauth_destroy(void *user_data)
{
    struct oauth_state *state = (struct oauth_state *)user_data;
    free(state->uc_endpoint);
    free(state->workspace_id);
    free(state->table);
    free(state->client_id);
    zb_secure_free_cstr(state->client_secret);
    free(state);
}

static const zerobus_headers_provider_vtable_t oauth_vtable = {
    oauth_get_headers,
    NULL,
    oauth_destroy,
};

zerobus_status_t
zb_oauth_headers_provider_new(const char *uc_endpoint, const char *workspace_id,
                              const char *table, const char *client_id,
                              const char *client_secret,
                              zerobus_headers_provider_t **out_provider)
{
    struct oauth_state *state = (struct oauth_state *)calloc(1, sizeof(*state));
    if (state == NULL) {
        ZB_ERROR("OAuth provider allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    state->uc_endpoint = zb_strdup(uc_endpoint);
    state->workspace_id = zb_strdup(workspace_id);
    state->table = zb_strdup(table);
    state->client_id = zb_strdup(client_id);
    state->client_secret = zb_strdup(client_secret);
    if (state->uc_endpoint == NULL || state->workspace_id == NULL ||
        state->table == NULL || state->client_id == NULL ||
        state->client_secret == NULL) {
        ZB_ERROR("OAuth provider configuration allocation failed");
        oauth_destroy(state);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status =
        zerobus_headers_provider_new(&oauth_vtable, state, out_provider, NULL);
    if (status != ZEROBUS_STATUS_OK) {
        oauth_destroy(state);
    }
    return status;
}
