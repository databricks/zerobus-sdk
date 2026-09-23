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
