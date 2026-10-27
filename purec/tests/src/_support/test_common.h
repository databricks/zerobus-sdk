/*
 * Shared support for the offline unit tests (no external framework): assertion
 * macros, the failure counter, and helpers common to more than one test file.
 */
#ifndef ZB_TEST_COMMON_H
#define ZB_TEST_COMMON_H

#include <stdio.h> // IWYU pragma: keep (fprintf)

#include "zerobus/zerobus.h" // IWYU pragma: keep (zerobus_string_view)

static int zb_test_failures = 0;

#define CHECK_IMPL(cond, cond_text, on_failure)                                \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__,          \
                    (cond_text));                                              \
            zb_test_failures++;                                                \
            on_failure;                                                        \
        }                                                                      \
    } while (0)

#define CHECK_INT_IMPL(a, b, a_text, b_text, on_failure)                       \
    do {                                                                       \
        long long zb_test_a_ = (long long)(a);                                 \
        long long zb_test_b_ = (long long)(b);                                 \
        if (zb_test_a_ != zb_test_b_) {                                        \
            fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n",          \
                    __FILE__, __LINE__, (a_text), zb_test_a_, (b_text),        \
                    zb_test_b_);                                               \
            zb_test_failures++;                                                \
            on_failure;                                                        \
        }                                                                      \
    } while (0)

#define CHECK(cond) CHECK_IMPL((cond), #cond, (void)0)
#define CHECK_EQ_INT(a, b) CHECK_INT_IMPL((a), (b), #a, #b, (void)0)
#define CHECK_OK(call) CHECK_EQ_INT(call, ZEROBUS_STATUS_OK)

/* Fatal counterpart to CHECK: record the failure, then jump to the test's
 * per-function `zb_cleanup:` label so its resources are still freed. Use it
 * for preconditions only — a NULL handle, a failed build/alloc — and keep CHECK
 * for independent value assertions. A test that uses REQUIRE must declare its
 * owned resources NULL-initialized at the top and end with a single
 * `zb_cleanup:` label that frees them unconditionally. */
#define REQUIRE(cond) CHECK_IMPL((cond), #cond, goto zb_cleanup)
#define REQUIRE_OK(call)                                                       \
    CHECK_INT_IMPL((call), ZEROBUS_STATUS_OK, #call, "ZEROBUS_STATUS_OK",      \
                   goto zb_cleanup)

#define TEST_MAIN_RETURN() return zb_test_failures == 0 ? 0 : 1

#define sv(s) zerobus_string_view(s)

#endif /* ZB_TEST_COMMON_H */
