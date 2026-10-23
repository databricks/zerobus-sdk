/*
 * Zerobus Pure C SDK — headers providers.
 *
 * A stream sends request headers each time it opens a gRPC call. A headers
 * provider supplies them through application callbacks.
 *
 * The SDK sends these headers itself, and a provider must not supply them:
 *   - :method, :scheme, :authority and :path
 *   - content-type and te
 *   - x-databricks-zerobus-table-name
 * The SDK adds no authorization header itself, so a provider normally supplies
 * just that.
 *
 * A header name must be a lowercase gRPC metadata key: letters, digits, '-',
 * '_' and '.', not starting with "grpc-". The connection headers host,
 * connection, keep-alive, proxy-connection, transfer-encoding, upgrade and
 * content-length are not allowed. A value must be printable ASCII. A stream
 * checks these rules when it opens a call, and an invalid header fails the
 * open with INVALID_ARGUMENT.
 */
#ifndef ZEROBUS_HEADERS_PROVIDER_H
#define ZEROBUS_HEADERS_PROVIDER_H

#include "zerobus/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Adds a header to the set that a get_headers callback fills. Both name and
 * value are copied. The value may be empty.
 *
 * Returns INVALID_ARGUMENT, and adds nothing, for an empty name, or for a name
 * or value that is not valid UTF-8.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_headers_add(zerobus_headers_t *headers, zerobus_string_view_t name,
                    zerobus_string_view_t value, zerobus_error_t **out_error);

/*
 * Called each time a stream opens a gRPC call, to collect the headers to send.
 * headers is valid only during the callback. On OK the added headers are sent.
 * Any other status fails the open with that status, and the added headers are
 * discarded.
 *
 * For example, a provider that sends two headers:
 *
 *     static zerobus_status_t get_headers(void *user_data,
 *                                         zerobus_headers_t *headers)
 *     {
 *         zerobus_status_t status =
 *             zerobus_headers_add(headers, zerobus_string_view("name1"),
 *                                 zerobus_string_view("value1"), NULL);
 *         if (status == ZEROBUS_STATUS_OK) {
 *             status =
 *                 zerobus_headers_add(headers, zerobus_string_view("name2"),
 *                                     zerobus_string_view("value2"), NULL);
 *         }
 *         return status;
 *     }
 */
typedef zerobus_status_t(ZEROBUS_CALL *zerobus_get_headers_fn)(
    void *user_data, zerobus_headers_t *headers);

/* Called when the server rejects the credentials that the last get_headers
 * call added. */
typedef void(ZEROBUS_CALL *zerobus_invalidate_headers_fn)(void *user_data);

/* Called once, when the provider's last reference is released. No callback
 * runs after it. */
typedef void(ZEROBUS_CALL *zerobus_destroy_user_data_fn)(void *user_data);

/*
 * The callbacks of a headers provider. get_headers is required, invalidate and
 * destroy may be NULL.
 */
typedef struct zerobus_headers_provider_vtable {
    zerobus_get_headers_fn get_headers;
    zerobus_invalidate_headers_fn invalidate;
    zerobus_destroy_user_data_fn destroy;
} zerobus_headers_provider_vtable_t;

/*
 * Creates a provider that calls the vtable's callbacks with user_data. The
 * vtable is copied, so it may be stack storage. A provider is reference
 * counted. Each stream that uses it holds its own reference, so the caller may
 * free the provider before those streams end.
 *
 * A provider serves one table and one identity. They run on SDK threads, and
 * streams that share a provider may call them concurrently.
 *
 * If creation fails, destroy is not called and user_data stays with the
 * caller.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_headers_provider_new(
    const zerobus_headers_provider_vtable_t *vtable, void *user_data,
    zerobus_headers_provider_t **out_provider, zerobus_error_t **out_error);

/* Releases the caller's reference. Releasing the last reference calls
 * destroy. */
ZEROBUS_API void ZEROBUS_CALL
zerobus_headers_provider_free(zerobus_headers_provider_t *provider);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* ZEROBUS_HEADERS_PROVIDER_H */
