/*
 * Zerobus Pure C SDK — SDK builder and SDK handle.
 */
#ifndef ZEROBUS_SDK_H
#define ZEROBUS_SDK_H

#include "zerobus/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * An SDK may be shared by threads creating separate stream builders.
 * Each SDK builder requires exclusive access. Concurrent calls must use
 * separate output/error slots.
 *
 * SDK builder. The endpoint setters validate the URL and copy their inputs, so
 * a malformed endpoint is rejected at set time. The builder is not consumed by
 * build and must be freed separately.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_sdk_builder_new(
    zerobus_sdk_builder_t **out_builder, zerobus_error_t **out_error);

ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_sdk_builder_set_endpoint(
    zerobus_sdk_builder_t *builder, zerobus_string_view_t zerobus_endpoint,
    zerobus_error_t **out_error);

ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_sdk_builder_set_unity_catalog_endpoint(
    zerobus_sdk_builder_t *builder,
    zerobus_string_view_t unity_catalog_endpoint, zerobus_error_t **out_error);

/* Name of the calling application, appended to the user-agent header after the
 * SDK's own "zerobus-sdk-c/<version>". Surrounding ASCII whitespace is
 * trimmed, and a blank name clears it. Must be printable ASCII. Default
 * none. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_sdk_builder_set_application_name(zerobus_sdk_builder_t *builder,
                                         zerobus_string_view_t application_name,
                                         zerobus_error_t **out_error);

/* Each stream built from this SDK refreshes its OAuth token once it is this
 * close to expiring. Default 300 seconds. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_sdk_builder_set_token_refresh_buffer_ms(zerobus_sdk_builder_t *builder,
                                                uint64_t buffer_ms,
                                                zerobus_error_t **out_error);

/* Whether each stream gets its own connection. When false, streams from this
 * SDK share one connection. Default true. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_sdk_builder_set_connection_per_stream(zerobus_sdk_builder_t *builder,
                                              bool enabled,
                                              zerobus_error_t **out_error);

ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_sdk_builder_build(const zerobus_sdk_builder_t *builder,
                          zerobus_sdk_t **out_sdk, zerobus_error_t **out_error);

ZEROBUS_API void ZEROBUS_CALL
zerobus_sdk_builder_free(zerobus_sdk_builder_t *builder);

/*
 * Release the caller's reference. Streams and stream builders keep the SDK
 * alive until they are freed. Do not use the released reference again or race
 * this call with its use. Final cleanup is best-effort and cannot report
 * failures.
 */
ZEROBUS_API void ZEROBUS_CALL zerobus_sdk_free(zerobus_sdk_t *sdk);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* ZEROBUS_SDK_H */
