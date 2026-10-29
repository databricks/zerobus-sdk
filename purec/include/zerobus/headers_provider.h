/*
 * Zerobus Pure C SDK — headers providers.
 *
 * A stream sends request headers each time it opens a gRPC call. A headers
 * provider supplies them, through application callbacks or from a fixed list.
 *
 * The SDK sends these headers itself, and a provider must not supply them:
 *   - :method, :scheme, :authority and :path
 *   - content-type and te
 *   - x-databricks-zerobus-table-name
 * The SDK adds no authorization header itself, so a provider normally supplies
 * just that.
 *
 * A header name must be a gRPC metadata key: letters, digits, '-', '_' and
 * '.', in any case, not starting with "grpc-". The connection headers host,
 * connection, keep-alive, proxy-connection, transfer-encoding, upgrade and
 * content-length are not allowed. A value must be printable ASCII.
 * zerobus_headers_add checks these rules.
 */
#ifndef ZEROBUS_HEADERS_PROVIDER_H
#define ZEROBUS_HEADERS_PROVIDER_H

#include "zerobus/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Adds a header to the set that a get_headers callback fills. Both name and
 * value are copied, the name in lowercase. The value may be empty.
 *
 * Returns INVALID_ARGUMENT, and adds nothing, for a header that breaks the
 * rules at the top of this file.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_headers_add(zerobus_headers_t *headers, zerobus_string_view_t name,
                    zerobus_string_view_t value, zerobus_error_t **out_error);

/*
 * Provider callbacks. get_headers and invalidate are called on SDK threads, and
 * streams that share a provider may call them concurrently. get_headers may
 * block, for example to fetch a token; invalidate must not. destroy runs on
 * whichever thread releases the last reference. Callbacks may call functions
 * such as zerobus_headers_add, but not functions on an SDK or a stream.
 */

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
 * Creates a provider that calls its callbacks with user_data. get_headers is
 * required, and destroy may be NULL. A provider serves one table and one
 * identity. It is reference counted. Each stream that uses it holds its own
 * reference, so the caller may free the provider before those streams end.
 * Streams may share a provider, but one provider per stream is recommended, so
 * that invalidate refers to that stream's last get_headers call.
 *
 * Other callbacks are optional, and each has a zerobus_headers_provider_set_*
 * setter. Call the setters before passing the provider on: while a builder or
 * stream holds it, they return FAILED_PRECONDITION.
 *
 * If creation fails, destroy is not called and user_data stays with the
 * caller.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_headers_provider_new(
    zerobus_get_headers_fn get_headers, zerobus_destroy_user_data_fn destroy,
    void *user_data, zerobus_headers_provider_t **out_provider,
    zerobus_error_t **out_error);

/* Sets the provider's invalidate callback, or with NULL removes it. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_headers_provider_set_invalidate(
    zerobus_headers_provider_t *provider,
    zerobus_invalidate_headers_fn invalidate, zerobus_error_t **out_error);

/*
 * Creates a provider that sends the same count headers on every call: names[i]
 * with values[i]. Both arrays and their strings are copied, and each header is
 * checked as by zerobus_headers_add. names and values may be NULL when count
 * is 0.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_static_headers_provider_new(
    const zerobus_string_view_t *names, const zerobus_string_view_t *values,
    size_t count, zerobus_headers_provider_t **out_provider,
    zerobus_error_t **out_error);

/* Releases the caller's reference. Releasing the last reference calls
 * destroy. */
ZEROBUS_API void ZEROBUS_CALL
zerobus_headers_provider_free(zerobus_headers_provider_t *provider);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* ZEROBUS_HEADERS_PROVIDER_H */
