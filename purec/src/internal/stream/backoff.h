/*
 * Delay between stream creation and recovery attempts. Mirrors
 * recovery_backoff_delay in rust/sdk/src/backoff.rs.
 *
 * Streams that fail together, for example when the server rate-limits stream
 * creation, retry together if they all wait the same fixed interval, and then
 * fail together again. Doubling the delay after each failure and adding a
 * random amount to it spreads those retries out over time.
 */
#ifndef ZB_STREAM_BACKOFF_H
#define ZB_STREAM_BACKOFF_H

#include <stdint.h>

/* The un-jittered delay stops doubling at this value, unless the configured
 * recovery backoff is already larger. Matches MAX_RECOVERY_BACKOFF_MS in
 * rust/sdk/src/backoff.rs. */
#define ZB_MAX_RECOVERY_BACKOFF_MS 15000u

/* How long to wait, in milliseconds, before retry number retry_index (0 for
 * the first retry). The un-jittered delay is base_ms * 2^retry_index, capped
 * at the larger of base_ms and ZB_MAX_RECOVERY_BACKOFF_MS. A uniformly random
 * jitter of up to that delay is added, so the result lies in
 * [delay, 2 * delay). It is never shorter than base_ms, saturates at
 * UINT64_MAX, and a base_ms of 0 always gives 0. */
uint64_t zb_recovery_backoff_ms(uint64_t base_ms, uint32_t retry_index);

#ifdef ZB_TESTING
/* zb_recovery_backoff_ms with the jitter drawn from random instead of from
 * the random number generator: the jitter is random modulo the delay. */
uint64_t zb_recovery_backoff_with_random_ms(uint64_t base_ms,
                                            uint32_t retry_index,
                                            uint64_t random);
#endif

#endif /* ZB_STREAM_BACKOFF_H */
