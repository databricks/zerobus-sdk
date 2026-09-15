/*
 * Zerobus Pure C SDK — error objects.
 */
#ifndef ZEROBUS_ERROR_H
#define ZEROBUS_ERROR_H

#include "zerobus/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Out-parameter contract for every failable function.
 *
 * A failable function returns a zerobus_status_t. Details come back through
 * out-parameters: an error on failure, and (for functions that create one) a
 * handle on success.
 *
 * Error — `zerobus_error_t **out_error`:
 *
 *   - Pass NULL to discard the error detail. The status code is still returned.
 *   - Otherwise `*out_error` MUST be NULL on entry. On failure the function
 *     might set it to an owned error that the caller must free with
 *     zerobus_error_free. On success it is left NULL.
 *
 * A non-NULL `*out_error` on entry can only mean a leaked or reused error, so
 * the function refuses the call: it returns ZEROBUS_STATUS_INVALID_ARGUMENT and
 * leaves the existing error untouched. This surfaces the mistake instead of
 * silently overwriting (and leaking) the previous error.
 *
 * When reusing one variable across calls, reset it to NULL between them:
 *
 *     zerobus_error_t *err = NULL;
 *     if (zerobus_stream_flush(stream, &err) != ZEROBUS_STATUS_OK) {
 *         // inspect err ...
 *         zerobus_error_free(err);
 *         err = NULL;               // required before passing &err again
 *     }
 *
 * Handle — e.g. `zerobus_sdk_t **out_sdk`:
 *
 *   Written only on success, left untouched on failure. Unlike out_error, the
 *   handle need not be NULL on entry. On success it overwrites the slot without
 *   freeing any prior value, so do not pass a variable that still holds a
 *   handle you need.
 */

/* The returned view is borrowed from the error, valid only until it is freed.
 */
ZEROBUS_API zerobus_string_view_t ZEROBUS_CALL
zerobus_error_message(const zerobus_error_t *error);

/* The status code carried by the error. Returns ZEROBUS_STATUS_UNKNOWN for a
 * NULL error. */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL
zerobus_error_status(const zerobus_error_t *error);

/* Whether retrying the failed operation may succeed (a transient failure).
 * Derived from the status code. False for a NULL error.
 *
 * Temporary status-based classification until the transport core defines
 * retryability precisely. */
ZEROBUS_API bool ZEROBUS_CALL
zerobus_error_is_retryable(const zerobus_error_t *error);

ZEROBUS_API void ZEROBUS_CALL zerobus_error_free(zerobus_error_t *error);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* ZEROBUS_ERROR_H */
