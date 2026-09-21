/*
 * Shared support for the offline unit tests (no external framework): assertion
 * macros, the failure counter, and helpers common to more than one test file.
 */
#ifndef ZB_TEST_COMMON_H
#define ZB_TEST_COMMON_H

#include <stdio.h> // IWYU pragma: keep (fprintf)

#include "zerobus/zerobus.h" // IWYU pragma: keep (zerobus_string_view)

static int zb_test_failures = 0;

#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);  \
            zb_test_failures++;                                                \
        }                                                                      \
    } while (0)

#define CHECK_EQ_INT(a, b)                                                     \
    do {                                                                       \
        long long _a = (long long)(a), _b = (long long)(b);                    \
        if (_a != _b) {                                                        \
            fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n",          \
                    __FILE__, __LINE__, #a, _a, #b, _b);                       \
            zb_test_failures++;                                                \
        }                                                                      \
    } while (0)

/* Fatal counterpart to CHECK: record the failure, then jump to the test's
 * per-function `zb_cleanup:` label so its resources are still freed. Use it
 * for preconditions only — a NULL handle, a failed build/alloc — and keep CHECK
 * for independent value assertions. A test that uses REQUIRE must declare its
 * owned resources NULL-initialized at the top and end with a single
 * `zb_cleanup:` label that frees them unconditionally. */
#define REQUIRE(cond)                                                          \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);  \
            zb_test_failures++;                                                \
            goto zb_cleanup;                                                   \
        }                                                                      \
    } while (0)

#define CHECK_OK(call) CHECK_EQ_INT(call, ZEROBUS_STATUS_OK)

#define REQUIRE_OK(call)                                                       \
    do {                                                                       \
        zerobus_status_t result_ = (call);                                     \
        CHECK_OK(result_);                                                     \
        if (result_ != ZEROBUS_STATUS_OK) {                                    \
            goto cleanup;                                                      \
        }                                                                      \
    } while (0)

#define TEST_MAIN_RETURN() return zb_test_failures == 0 ? 0 : 1

#define sv(s) zerobus_string_view(s)

#endif /* ZB_TEST_COMMON_H */
