#include "backoff.h"

#include <openssl/rand.h>

#include "internal/common/utils.h"

ZB_STATIC uint64_t zb_recovery_backoff_with_random_ms(uint64_t base_ms,
                                                      uint32_t retry_index,
                                                      uint64_t random)
{
    if (base_ms == 0) {
        return 0;
    }
    uint64_t cap_ms = base_ms > ZB_MAX_RECOVERY_BACKOFF_MS
                          ? base_ms
                          : ZB_MAX_RECOVERY_BACKOFF_MS;
    /* base_ms << retry_index, without overflowing, capped at cap_ms. */
    uint64_t delay_ms = (retry_index >= 64 || base_ms > (cap_ms >> retry_index))
                            ? cap_ms
                            : base_ms << retry_index;
    uint64_t jitter_ms = random % delay_ms;
    return delay_ms > UINT64_MAX - jitter_ms ? UINT64_MAX
                                             : delay_ms + jitter_ms;
}

uint64_t zb_recovery_backoff_ms(uint64_t base_ms, uint32_t retry_index)
{
    uint64_t random = 0;
    /* RAND_bytes is thread-safe. If it fails, the wait is the un-jittered
     * delay: still never shorter than the backoff, only not spread out. */
    if (RAND_bytes((unsigned char *)&random, (int)sizeof(random)) != 1) {
        random = 0;
    }
    return zb_recovery_backoff_with_random_ms(base_ms, retry_index, random);
}
