#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "concurrency.h"
#include "log.h"

static struct {
    enum zb_log_level level;
    FILE *output;
    zb_once_t init_once;
    zb_mutex_t write_mutex;
} config = {.level = ZB_LOG_OFF,
            .output = NULL,
            .init_once = ZB_ONCE_INIT,
            .write_mutex = ZB_MUTEX_INITIALIZER};

static const char *const level_names[] = {"OFF",  "ERROR", "WARN",
                                          "INFO", "DEBUG", "TRACE"};

static void zb_log_init(void)
{
    int saved_errno = errno;
    config.output = stderr;
    const char *value = getenv("ZEROBUS_LOG_LEVEL");
    if (value != NULL && value[0] != '\0') {
        static const char *const values[] = {"off",  "error", "warn",
                                             "info", "debug", "trace"};
        size_t i;
        for (i = 0; i < sizeof(values) / sizeof(values[0]); ++i) {
            if (strcmp(value, values[i]) == 0) {
                config.level = (enum zb_log_level)i;
                break;
            }
        }
        if (i == sizeof(values) / sizeof(values[0])) {
            (void)fputs(
                "[zerobus][WARN] zb_log_init: invalid ZEROBUS_LOG_LEVEL: "
                "logging disabled\n",
                stderr);
            (void)fflush(stderr);
        }
    }
    if (config.level != ZB_LOG_OFF) {
        const char *path = getenv("ZEROBUS_LOG_FILE");
        if (path != NULL && path[0] != '\0') {
            /* The stream remains open until normal process exit. */
            FILE *output = fopen(path, "a");
            if (output != NULL) {
                config.output = output;
            } else {
                (void)fputs("[zerobus][WARN] zb_log_init: cannot open "
                            "ZEROBUS_LOG_FILE: using stderr\n",
                            stderr);
                (void)fflush(stderr);
            }
        }
    }
    errno = saved_errno;
}

bool zb_log_enabled(enum zb_log_level level)
{
    (void)zb_once(&config.init_once, zb_log_init);
    return level > ZB_LOG_OFF && level <= ZB_LOG_TRACE && level <= config.level;
}

void zb_log_write(enum zb_log_level level, const char *function,
                  const char *fmt, ...)
{
    if (config.output == NULL) {
        return;
    }
    int saved_errno = errno;
    (void)zb_mutex_lock(&config.write_mutex);
    (void)fprintf(config.output, "[zerobus][%s] %s: ", level_names[level],
                  function);
    va_list ap;
    va_start(ap, fmt);
    int written = vfprintf(config.output, fmt, ap);
    va_end(ap);
    if (written < 0) {
        (void)fputs("format failed", config.output);
    }
    (void)fputc('\n', config.output);
    (void)fflush(config.output);
    (void)zb_mutex_unlock(&config.write_mutex);
    errno = saved_errno;
}
