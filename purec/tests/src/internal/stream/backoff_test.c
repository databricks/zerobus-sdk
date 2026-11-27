/* Unit tests for the recovery backoff in backoff.c, which mirrors
 * recovery_backoff_delay in rust/sdk/src/backoff.rs. */
#include <stdbool.h>
#include <stdint.h>

#include "internal/stream/backoff.h"
#include "test_common.h"

/* ---- helpers ----------------------------------------------------------- */

#define SAMPLES 200

/* Whether every drawn wait for retry_index lies in [floor_ms, 2 * floor_ms). */
static bool draws_in_range(uint64_t base_ms, uint32_t retry_index,
                           uint64_t floor_ms)
{
    for (int i = 0; i < SAMPLES; ++i) {
        uint64_t delay = zb_recovery_backoff_ms(base_ms, retry_index);
        if (delay < floor_ms || delay >= 2 * floor_ms) {
            return false;
        }
    }
    return true;
}

/* ---- tests ------------------------------------------------------------- */

static void test_doubles_per_retry_and_never_drops_below_base(void)
{
    CHECK(draws_in_range(1000, 0, 1000));
    CHECK(draws_in_range(1000, 1, 2000));
    CHECK(draws_in_range(1000, 2, 4000));
    CHECK(draws_in_range(1000, 3, 8000));
}

static void test_stops_doubling_at_cap(void)
{
    CHECK(draws_in_range(1000, 4, ZB_MAX_RECOVERY_BACKOFF_MS));
    CHECK(draws_in_range(1000, 30, ZB_MAX_RECOVERY_BACKOFF_MS));
    CHECK(draws_in_range(1, 63, ZB_MAX_RECOVERY_BACKOFF_MS));
    CHECK(draws_in_range(1, 64, ZB_MAX_RECOVERY_BACKOFF_MS));
    CHECK(draws_in_range(1, UINT32_MAX, ZB_MAX_RECOVERY_BACKOFF_MS));
}

static void test_base_above_cap_is_not_reduced(void)
{
    CHECK(draws_in_range(60000, 0, 60000));
    CHECK(draws_in_range(60000, 5, 60000));
}

static void test_zero_base_never_waits(void)
{
    CHECK(zb_recovery_backoff_ms(0, 0) == 0);
    CHECK(zb_recovery_backoff_ms(0, 10) == 0);
    CHECK(zb_recovery_backoff_ms(0, UINT32_MAX) == 0);
}

/* The jitter is the random draw modulo the un-jittered delay. */
static void test_jitter_comes_from_the_draw(void)
{
    CHECK(zb_recovery_backoff_with_random_ms(1000, 0, 0) == 1000);
    CHECK(zb_recovery_backoff_with_random_ms(1000, 0, 999) == 1999);
    CHECK(zb_recovery_backoff_with_random_ms(1000, 0, 1000) == 1000);
    CHECK(zb_recovery_backoff_with_random_ms(1000, 2, 1) == 4001);
    CHECK(zb_recovery_backoff_with_random_ms(1000, 9, UINT64_MAX) ==
          ZB_MAX_RECOVERY_BACKOFF_MS + UINT64_MAX % ZB_MAX_RECOVERY_BACKOFF_MS);
}

static void test_extreme_inputs_saturate(void)
{
    CHECK(zb_recovery_backoff_with_random_ms(UINT64_MAX, UINT32_MAX,
                                             UINT64_MAX - 1) == UINT64_MAX);
    CHECK(zb_recovery_backoff_with_random_ms(UINT64_MAX, 0, 0) == UINT64_MAX);
}

static void test_is_jittered(void)
{
    uint64_t first = zb_recovery_backoff_ms(1000, 0);
    bool differs = false;
    for (int i = 0; i < SAMPLES && !differs; ++i) {
        differs = zb_recovery_backoff_ms(1000, 0) != first;
    }
    CHECK(differs);
}

int main(void)
{
    test_doubles_per_retry_and_never_drops_below_base();
    test_stops_doubling_at_cap();
    test_base_above_cap_is_not_reduced();
    test_zero_base_never_waits();
    test_jitter_comes_from_the_draw();
    test_extreme_inputs_saturate();
    test_is_jittered();
    TEST_MAIN_RETURN();
}
