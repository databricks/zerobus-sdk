#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "log.h"

static bool initialized;
static enum zb_log_level configured_level = ZB_LOG_OFF;

static const char *const level_names[] = {"OFF",  "ERROR", "WARN",
                                          "INFO", "DEBUG", "TRACE"};

static void zb_log_init(void)
{
    int saved_errno = errno;
    initialized = true;
    const char *value = getenv("ZEROBUS_LOG_LEVEL");
    if (value != NULL && value[0] != '\0') {
        static const char *const values[] = {"off",  "error", "warn",
                                             "info", "debug", "trace"};
        size_t i;
        for (i = 0; i < sizeof(values) / sizeof(values[0]); ++i) {
            if (strcmp(value, values[i]) == 0) {
                configured_level = (enum zb_log_level)i;
                break;
            }
        }
        if (i == sizeof(values) / sizeof(values[0])) {
            (void)fputs(
                "[zerobus][WARN] zb_log_init: invalid ZEROBUS_LOG_LEVEL: "
                "logging disabled\n",
                stderr);
        }
    }
    errno = saved_errno;
}

bool zb_log_enabled(enum zb_log_level level)
{
    if (!initialized) {
        zb_log_init();
    }
    return level > ZB_LOG_OFF && level <= ZB_LOG_TRACE &&
           level <= configured_level;
}

void zb_log_write(enum zb_log_level level, const char *function,
                  const char *fmt, ...)
{
    int saved_errno = errno;
    (void)fprintf(stderr, "[zerobus][%s] %s: ", level_names[level], function);
    va_list ap;
    va_start(ap, fmt);
    int written = vfprintf(stderr, fmt, ap);
    va_end(ap);
    if (written < 0) {
        (void)fputs("format failed", stderr);
    }
    (void)fputc('\n', stderr);
    errno = saved_errno;
}
