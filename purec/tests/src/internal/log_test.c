/* CTest supplies INFO and a dedicated build-local log file for this process. */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../test_common.h"
#include "internal/log.h"

static void test_write_before_init_is_a_noop(void)
{
    errno = EDOM;
    zb_log_write(ZB_LOG_ERROR, __func__,
                 "pre-init message must not be written");
    CHECK_EQ_INT(errno, EDOM);
}

static void test_macros(void)
{
    int evaluated[3] = {0};
    int suppressed = 0;
    errno = EDOM;
    /* A suppressed first call must still initialize the logger. */
    ZB_DEBUG("suppressed %d", ++suppressed);
    CHECK_EQ_INT(errno, EDOM);
    CHECK_EQ_INT(suppressed, 0);

    errno = ERANGE;
    ZB_ERROR("number=%d text=%s", ++evaluated[0], "example");
    ZB_WARN("warning %d", ++evaluated[1]);
    ZB_INFO("info %d", ++evaluated[2]);
    ZB_TRACE("suppressed %d", ++suppressed);
    ZB_LOG(ZB_LOG_OFF, "suppressed %d", ++suppressed);
    ZB_LOG((enum zb_log_level) - 1, "suppressed %d", ++suppressed);
    ZB_LOG((enum zb_log_level)(ZB_LOG_TRACE + 1), "suppressed %d",
           ++suppressed);
    CHECK_EQ_INT(errno, ERANGE);
    CHECK_EQ_INT(suppressed, 0);
    for (int i = 0; i < 3; ++i) {
        CHECK_EQ_INT(evaluated[i], 1);
    }

    /* Check dangling-else safety and format-only macro calls. */
    int else_taken = 0;
    if (1)
        ZB_DEBUG("suppressed");
    else
        ++else_taken;
    CHECK_EQ_INT(else_taken, 0);
    if (0)
        ZB_ERROR("unreachable");
    else
        ZB_INFO("format-only message");
}

static void test_level_filter(void)
{
    for (int level = -1; level <= ZB_LOG_TRACE + 1; ++level) {
        errno = EDOM;
        bool enabled = zb_log_enabled((enum zb_log_level)level);
        CHECK_EQ_INT(errno, EDOM);
        CHECK_EQ_INT(enabled, level >= ZB_LOG_ERROR && level <= ZB_LOG_INFO);
    }
}

static void test_writer(void)
{
    errno = ERANGE;
    zb_log_write(ZB_LOG_TRACE, __func__, "direct %d", 42);
    CHECK_EQ_INT(errno, ERANGE);
}

static void test_log_file(void)
{
    FILE *file = NULL;
    const char *path = getenv("ZEROBUS_LOG_FILE");
    char output[1024];
    const char *expected =
        "seed\n"
        "[zerobus][ERROR] test_macros: number=1 text=example\n"
        "[zerobus][WARN] test_macros: warning 1\n"
        "[zerobus][INFO] test_macros: info 1\n"
        "[zerobus][INFO] test_macros: format-only message\n"
        "[zerobus][TRACE] test_writer: direct 42\n";

    REQUIRE(path != NULL && path[0] != '\0');
    /* Seed before initialization: append must preserve existing contents. */
    file = fopen(path, "w");
    REQUIRE(file != NULL);
    REQUIRE(fputs("seed\n", file) >= 0);
    int closed = fclose(file);
    file = NULL;
    REQUIRE(closed == 0);

    test_write_before_init_is_a_noop();
    test_macros();
    test_level_filter();
    test_writer();

    /* Read while the logger is open to verify each write was flushed. */
    file = fopen(path, "r");
    REQUIRE(file != NULL);
    size_t length = fread(output, 1, sizeof(output) - 1, file);
    output[length] = '\0';
    CHECK(!ferror(file));
    CHECK(feof(file));
    CHECK_EQ_INT(length, strlen(expected));
    CHECK(strcmp(output, expected) == 0);

zb_cleanup:
    if (file != NULL) {
        fclose(file);
    }
    /* The logger keeps its own stream open */
}

int main(void)
{
    test_log_file();
    TEST_MAIN_RETURN();
}
