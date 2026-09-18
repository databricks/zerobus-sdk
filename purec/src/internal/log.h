#ifndef ZB_LOG_H
#define ZB_LOG_H

#include <stdbool.h>

enum zb_log_level {
    ZB_LOG_OFF = 0,
    ZB_LOG_ERROR = 1,
    ZB_LOG_WARN = 2,
    ZB_LOG_INFO = 3,
    ZB_LOG_DEBUG = 4,
    ZB_LOG_TRACE = 5
};

bool zb_log_enabled(enum zb_log_level level);
/* Unfiltered output, level must be ERROR through TRACE. Use macros instead. */
void zb_log_write(enum zb_log_level level, const char *function,
                  const char *fmt, ...)
#if defined(__GNUC__)
    __attribute__((format(printf, 3, 4)))
#endif
    ;

/* Keep the guard outside the function call so disabled arguments are not
 * evaluated. Logging arguments must never perform required SDK work. */
#define ZB_LOG(level, ...)                                                     \
    do {                                                                       \
        if (zb_log_enabled(level)) {                                           \
            zb_log_write(level, __func__, __VA_ARGS__);                        \
        }                                                                      \
    } while (0)

#define ZB_ERROR(...) ZB_LOG(ZB_LOG_ERROR, __VA_ARGS__)
#define ZB_WARN(...) ZB_LOG(ZB_LOG_WARN, __VA_ARGS__)
#define ZB_INFO(...) ZB_LOG(ZB_LOG_INFO, __VA_ARGS__)
#define ZB_DEBUG(...) ZB_LOG(ZB_LOG_DEBUG, __VA_ARGS__)
#define ZB_TRACE(...) ZB_LOG(ZB_LOG_TRACE, __VA_ARGS__)

#endif /* ZB_LOG_H */
