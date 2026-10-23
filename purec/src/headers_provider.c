#include <stdatomic.h>
#include <stdlib.h>

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
