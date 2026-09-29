/*
 * Internal error construction, shared by every public entry point.
 */
#ifndef ZB_ERROR_H
#define ZB_ERROR_H

#include "zerobus/common.h"

struct zerobus_error {
    zerobus_status_t code;
    char *message;
    size_t message_len;
};

/*
 * Build an owned error with a printf-formatted message. Returns NULL if object
 * or message allocation, or message formatting, fails. Callers preserve their
 * original status when error details are unavailable.
 */
zerobus_error_t *zb_error_newf(zerobus_status_t code, const char *fmt, ...)
#if defined(__GNUC__)
    __attribute__((format(printf, 2, 3)))
#endif
    ;

/*
 * Convenience for the common "set *out_error (if non-NULL) and return status"
 * pattern used by every public entry point. When error allocation fails, leaves
 * *out_error as NULL. Always returns `code`.
 */
zerobus_status_t zb_fail(zerobus_error_t **out_error, zerobus_status_t code,
                         const char *fmt, ...)
#if defined(__GNUC__)
    __attribute__((format(printf, 3, 4)))
#endif
    ;

#endif /* ZB_ERROR_H */
