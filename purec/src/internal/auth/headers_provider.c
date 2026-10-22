#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "error.h"
#include "headers_provider.h"
#include "internal/common/utils.h"
#include "internal/network/grpc_utils.h"
#include "oauth.h"

typedef enum {
    PROVIDER_FIXED,
    PROVIDER_CUSTOM,
    PROVIDER_OAUTH
} provider_kind_t;

struct zerobus_headers_provider {
    atomic_size_t refs;
    provider_kind_t kind;
    zb_headers_t fixed;
    zerobus_headers_provider_vtable_t vtable;
    void *user_data;
    zb_oauth_t *oauth;
};

struct zerobus_headers_collector {
    zb_headers_t headers;
    size_t capacity;
};

/* ---- header copies ------------------------------------------------------ */

static void free_header(zb_metadata_t *header)
{
    /* One block holds the name, a NUL, the value and a NUL. */
    zb_secure_free((char *)header->name.data,
                   header->name.len + header->value.len + 2);
}

void zb_headers_clear(zb_headers_t *headers)
{
    if (!headers) {
        return;
    }
    for (size_t i = 0; i < headers->count; ++i) {
        free_header(&headers->items[i]);
    }
    free(headers->items);
    *headers = (zb_headers_t){0};
}

/* The SDK sets these itself. */
static bool sdk_owned(zerobus_string_view_t name)
{
    return zb_view_equals(name, "x-databricks-zerobus-table-name") ||
           zb_view_equals(name, "user-agent");
}

static zerobus_status_t add_header(zb_headers_t *headers, size_t *capacity,
                                   zerobus_string_view_t name,
                                   zerobus_string_view_t value)
{
    const zb_metadata_t header = {name, value};
    if (!zb_is_valid_grpc_metadata(&header) ||
        name.len > SIZE_MAX / 2 - value.len) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (sdk_owned(name)) {
        return ZEROBUS_STATUS_OK;
    }
    if (headers->count == *capacity) {
        size_t grown = *capacity ? *capacity * 2 : 4;
        zb_metadata_t *items = realloc(headers->items, grown * sizeof(*items));
        if (!items) {
            return ZEROBUS_STATUS_OUT_OF_MEMORY;
        }
        headers->items = items;
        *capacity = grown;
    }
    char *block = malloc(name.len + value.len + 2);
    if (!block) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    memcpy(block, name.data, name.len);
    block[name.len] = '\0';
    if (value.len) {
        memcpy(block + name.len + 1, value.data, value.len);
    }
    block[name.len + 1 + value.len] = '\0';
    headers->items[headers->count++] =
        (zb_metadata_t){{block, name.len}, {block + name.len + 1, value.len}};
    return ZEROBUS_STATUS_OK;
}

static zerobus_status_t copy_headers(const zb_metadata_t *items, size_t count,
                                     zb_headers_t *out)
{
    zb_headers_t copy = {0};
    size_t capacity = 0;
    for (size_t i = 0; i < count; ++i) {
        zerobus_status_t status =
            add_header(&copy, &capacity, items[i].name, items[i].value);
        if (status != ZEROBUS_STATUS_OK) {
            zb_headers_clear(&copy);
            return status;
        }
    }
    *out = copy;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_headers_collector_add(zerobus_headers_collector_t *collector,
                              zerobus_string_view_t name,
                              zerobus_string_view_t value)
{
    if (!collector) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    return add_header(&collector->headers, &collector->capacity, name, value);
}

/* ---- providers ---------------------------------------------------------- */

static zerobus_headers_provider_t *provider_new(provider_kind_t kind)
{
    zerobus_headers_provider_t *provider = calloc(1, sizeof(*provider));
    if (provider) {
        atomic_init(&provider->refs, 1);
        provider->kind = kind;
    }
    return provider;
}

zerobus_status_t zerobus_headers_provider_new(
    const zerobus_header_t *headers, size_t header_count,
    zerobus_headers_provider_t **out_provider, zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (out_provider == NULL || (headers == NULL && header_count != 0)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "out_provider must not be NULL and headers must not be "
                       "NULL when header_count is nonzero");
    }
    zerobus_headers_provider_t *provider = provider_new(PROVIDER_FIXED);
    if (!provider) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    size_t capacity = 0;
    for (size_t i = 0; i < header_count; ++i) {
        zerobus_status_t status = add_header(&provider->fixed, &capacity,
                                             headers[i].name, headers[i].value);
        if (status != ZEROBUS_STATUS_OK) {
            zerobus_headers_provider_free(provider);
            if (status == ZEROBUS_STATUS_INVALID_ARGUMENT) {
                return zb_fail(out_error, status,
                               "header %zu is not valid gRPC metadata", i);
            }
            return status;
        }
    }
    *out_provider = provider;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_custom_headers_provider_new(
    const zerobus_headers_provider_vtable_t *vtable, void *user_data,
    zerobus_headers_provider_t **out_provider, zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (out_provider == NULL || vtable == NULL ||
        vtable->struct_size < sizeof(*vtable) || vtable->get_headers == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "a vtable with struct_size and get_headers is required");
    }
    zerobus_headers_provider_t *provider = provider_new(PROVIDER_CUSTOM);
    if (!provider) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    provider->vtable = *vtable;
    provider->user_data = user_data;
    *out_provider = provider;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zb_oauth_headers_provider_new(const char *uc_endpoint, const char *workspace_id,
                              zerobus_string_view_t table,
                              zerobus_string_view_t client_id,
                              zerobus_string_view_t client_secret,
                              zerobus_headers_provider_t **out_provider)
{
    zerobus_headers_provider_t *provider = provider_new(PROVIDER_OAUTH);
    if (!provider) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status =
        zb_oauth_new(uc_endpoint, workspace_id, table, client_id, client_secret,
                     &provider->oauth);
    if (status != ZEROBUS_STATUS_OK) {
        free(provider);
        return status;
    }
    *out_provider = provider;
    return ZEROBUS_STATUS_OK;
}

zerobus_headers_provider_t *
zb_headers_provider_ref(zerobus_headers_provider_t *provider)
{
    atomic_fetch_add(&provider->refs, 1);
    return provider;
}

void zerobus_headers_provider_free(zerobus_headers_provider_t *provider)
{
    if (provider == NULL || atomic_fetch_sub(&provider->refs, 1) != 1) {
        return;
    }
    zb_headers_clear(&provider->fixed);
    if (provider->kind == PROVIDER_CUSTOM &&
        provider->vtable.destroy_user_data) {
        provider->vtable.destroy_user_data(provider->user_data);
    }
    zb_oauth_free(provider->oauth);
    free(provider);
}

zerobus_status_t zb_headers_provider_get(zerobus_headers_provider_t *provider,
                                         zerobus_string_view_t table,
                                         zb_deadline_t deadline,
                                         zb_headers_t *out,
                                         zerobus_error_t **out_error)
{
    switch (provider->kind) {
    case PROVIDER_FIXED:
        return copy_headers(provider->fixed.items, provider->fixed.count, out);
    case PROVIDER_CUSTOM: {
        zerobus_headers_collector_t collector = {0};
        zerobus_status_t status = provider->vtable.get_headers(
            provider->user_data, table, &collector);
        if (status != ZEROBUS_STATUS_OK) {
            zb_headers_clear(&collector.headers);
            return zb_fail(out_error, status,
                           "headers provider failed with status %u",
                           (unsigned)status);
        }
        *out = collector.headers;
        return ZEROBUS_STATUS_OK;
    }
    case PROVIDER_OAUTH:
    default: {
        char *token = NULL;
        zerobus_status_t status =
            zb_oauth_token(provider->oauth, deadline, &token, out_error);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        size_t len = strlen(token);
        char *value = malloc(len + 8);
        if (!value) {
            zb_secure_free_cstr(token);
            return ZEROBUS_STATUS_OUT_OF_MEMORY;
        }
        memcpy(value, "Bearer ", 7);
        memcpy(value + 7, token, len + 1);
        zb_secure_free_cstr(token);
        const zb_metadata_t header = {zerobus_string_view("authorization"),
                                      {value, len + 7}};
        status = copy_headers(&header, 1, out);
        zb_secure_free(value, len + 8);
        return status;
    }
    }
}

void zb_headers_provider_invalidate(zerobus_headers_provider_t *provider,
                                    zerobus_string_view_t table)
{
    if (provider->kind == PROVIDER_CUSTOM && provider->vtable.invalidate) {
        provider->vtable.invalidate(provider->user_data, table);
    } else if (provider->kind == PROVIDER_OAUTH) {
        zb_oauth_invalidate(provider->oauth);
    }
}
