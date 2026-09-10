/* Unit tests for the error object and its ownership contract. */
#include <string.h>

#include "error.h"
#include "test_common.h"

static void test_error_newf(void)
{
    zerobus_error_t *e = zb_error_newf(ZEROBUS_STATUS_INVALID_ARGUMENT,
                                       "bad value %d for %s", 42, "field");
    CHECK(e != NULL);
    zerobus_string_view_t m = zerobus_error_message(e);
    CHECK(m.len == strlen("bad value 42 for field"));
    CHECK(memcmp(m.data, "bad value 42 for field", m.len) == 0);
    zerobus_error_free(e);
}

static void test_error_message_null(void)
{
    zerobus_string_view_t m = zerobus_error_message(NULL);
    CHECK(m.data == NULL);
    CHECK_EQ_INT(m.len, 0);
}

static void test_error_free_null(void)
{
    zerobus_error_free(NULL); /* must be a no-op */
    CHECK(1);
}

static void test_fail_sets_error(void)
{
    zerobus_error_t *e = NULL;
    zerobus_status_t s =
        zb_fail(&e, ZEROBUS_STATUS_UNAVAILABLE, "temporary %s", "glitch");
    CHECK_EQ_INT(s, ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(e != NULL);
    zerobus_string_view_t m = zerobus_error_message(e);
    CHECK(m.len == strlen("temporary glitch"));
    zerobus_error_free(e);
}

static void test_fail_null_out(void)
{
    /* A NULL out_error slot must still return the status without crashing. */
    zerobus_status_t s = zb_fail(NULL, ZEROBUS_STATUS_INTERNAL, "no slot");
    CHECK_EQ_INT(s, ZEROBUS_STATUS_INTERNAL);
}

static void test_error_status(void)
{
    zerobus_error_t *e = zb_error_newf(ZEROBUS_STATUS_UNAVAILABLE, "temporary");
    CHECK(e != NULL);
    CHECK_EQ_INT(zerobus_error_status(e), ZEROBUS_STATUS_UNAVAILABLE);
    zerobus_error_free(e);

    /* A NULL error reports UNKNOWN. */
    CHECK_EQ_INT(zerobus_error_status(NULL), ZEROBUS_STATUS_UNKNOWN);
}

static void test_error_is_retryable(void)
{
    zerobus_error_t *retry = zb_error_newf(ZEROBUS_STATUS_UNAVAILABLE, "again");
    CHECK(retry != NULL);
    CHECK(zerobus_error_is_retryable(retry));
    zerobus_error_free(retry);

    zerobus_error_t *fatal =
        zb_error_newf(ZEROBUS_STATUS_INVALID_ARGUMENT, "nope");
    CHECK(fatal != NULL);
    CHECK(!zerobus_error_is_retryable(fatal));
    zerobus_error_free(fatal);

    /* Transient transport failures are retryable. */
    zerobus_error_t *internal = zb_error_newf(ZEROBUS_STATUS_INTERNAL, "blip");
    CHECK(internal != NULL);
    CHECK(zerobus_error_is_retryable(internal));
    zerobus_error_free(internal);

    /* A NULL error is not retryable. */
    CHECK(!zerobus_error_is_retryable(NULL));
}

int main(void)
{
    test_error_newf();
    test_error_message_null();
    test_error_free_null();
    test_fail_sets_error();
    test_fail_null_out();
    test_error_status();
    test_error_is_retryable();
    TEST_MAIN_RETURN();
}
