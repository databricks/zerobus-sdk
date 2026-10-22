/*
 * Zerobus Pure C SDK — headers providers: the request headers, such as
 * authorization, that a stream sends each time it opens a connection.
 */
#ifndef ZEROBUS_HEADERS_H
#define ZEROBUS_HEADERS_H

#include "zerobus/common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct zerobus_header {
    zerobus_string_view_t name;
    zerobus_string_view_t value;
} zerobus_header_t;

/*
 * Adds one header to the set being collected by a get_headers callback. The
 * name must be a lowercase gRPC metadata key (a-z, 0-9, '-', '_', '.'), not
 * grpc-* and not a name HTTP/2 or the transport reserves. The value must be
 * printable ASCII. Both are copied. Returns INVALID_ARGUMENT for an invalid
 * header. The SDK owns the table-name and user-agent headers and drops
 * provider values for them.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_headers_collector_add(
    zerobus_headers_collector_t *collector, zerobus_string_view_t name,
    zerobus_string_view_t value);

/*
 * Adds the headers for a stream on table_name through the collector, which
 * the SDK owns and frees after the call. Return OK, or the failure status, in
 * which case added headers are discarded and the open attempt fails with that
 * status. May run concurrently for different streams.
 */
typedef zerobus_status_t(ZEROBUS_CALL *zerobus_get_headers_fn)(
    void *user_data, zerobus_string_view_t table_name,
    zerobus_headers_collector_t *collector);

/* Called after the server rejected the headers' credentials, so cached
 * credentials can be dropped. Must not block. */
typedef void(ZEROBUS_CALL *zerobus_invalidate_headers_fn)(
    void *user_data, zerobus_string_view_t table_name);

typedef void(ZEROBUS_CALL *zerobus_destroy_user_data_fn)(void *user_data);

/*
 * struct_size must be sizeof(zerobus_headers_provider_vtable_t). get_headers
 * is required; invalidate and destroy_user_data may be NULL.
 */
typedef struct zerobus_headers_provider_vtable {
    size_t struct_size;
    zerobus_get_headers_fn get_headers;
    zerobus_invalidate_headers_fn invalidate;
    zerobus_destroy_user_data_fn destroy_user_data;
} zerobus_headers_provider_vtable_t;

/* A provider that always returns copies of the given headers, such as
 * {"authorization", "Bearer <token>"}. Copies the array and its strings. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_headers_provider_new(
    const zerobus_header_t *headers, size_t header_count,
    zerobus_headers_provider_t **out_provider, zerobus_error_t **out_error);

/*
 * A provider backed by callbacks. Copies the vtable. On success, a non-NULL
 * destroy_user_data is called exactly once, after the last stream or builder
 * using the provider is freed; with NULL, user_data must stay valid until
 * then.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_custom_headers_provider_new(
    const zerobus_headers_provider_vtable_t *vtable, void *user_data,
    zerobus_headers_provider_t **out_provider, zerobus_error_t **out_error);

/* Releases the caller's reference. Builders and streams that use the provider
 * keep it alive. NULL-safe. */
ZEROBUS_API void ZEROBUS_CALL
zerobus_headers_provider_free(zerobus_headers_provider_t *provider);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* ZEROBUS_HEADERS_H */
