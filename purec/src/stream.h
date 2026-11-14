#ifndef ZB_STREAM_H
#define ZB_STREAM_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "internal/landing_zone.h"
#include "internal/platform/concurrency.h"
#include "zerobus/common.h"

/* Stream options, each field is documented at its setter in zerobus/stream.h.
 */
typedef struct zb_stream_config {
    size_t max_inflight_requests;
    bool recovery;
    uint64_t recovery_timeout_ms;
    uint64_t recovery_backoff_ms;
    uint32_t recovery_retries;
    uint64_t server_lack_of_ack_timeout_ms;
    uint64_t flush_timeout_ms;
    uint64_t stream_paused_max_wait_time_ms;
    size_t max_ingest_payload_bytes;
} zb_stream_config_t;

#define ZB_MAX_INFLIGHT_REQUESTS ((size_t)1 << 24)

static const zb_stream_config_t zb_stream_config_default = {
    .max_inflight_requests = 1 << 16,
    .recovery = true,
    .recovery_timeout_ms = 15000,
    .recovery_backoff_ms = 2000,
    .recovery_retries = 4,
    .server_lack_of_ack_timeout_ms = 60000,
    .flush_timeout_ms = 300000,
    .stream_paused_max_wait_time_ms = UINT64_MAX,
    .max_ingest_payload_bytes = 10u * 1024u * 1024u - 64u * 1024u,
};

struct zerobus_stream {
    zerobus_sdk_t *sdk; /* owned reference */
    char *table;
    zb_stream_config_t config;
    zerobus_headers_provider_t *headers_provider; /* owned reference */
    zb_landing_zone_t *zone;

    atomic_bool closed; /* set by close, rejects ingest once true */

    /* Highest acknowledged offset, -1 before the first. Written by the
     * receiver, which then broadcasts acked_changed under mutex while
     * flush_waiters is nonzero. */
    _Atomic zerobus_offset_t acked;
    atomic_uint flush_waiters;
    zb_cond_t acked_changed;

    /* Guards the state the stream's threads share: the stream's own, and its
     * current attempt's fields marked so in internal/stream/workers.h. One
     * lock covers both so that the supervisor can wait for a change to either
     * in a single wait, such as a worker finishing or a close. changed is
     * broadcast on every change to it. */
    zb_mutex_t mutex;
    zb_cond_t changed;
    bool opened; /* The first open finished, with open_status. */
    zerobus_status_t open_status;
    zerobus_status_t failure; /* Terminal failure; OK while usable. */
    char *failure_message;
    bool finishing; /* Close flushed: end the call cleanly, then stop. */
    bool aborting;  /* Free, or a close whose flush failed: stop at once. */
    /* The first close's result, which later closes report. */
    bool close_done;
    zerobus_status_t close_status;
    char *close_message;
    zb_thread_t supervisor;
    bool supervisor_running; /* Started and not joined yet. */
};

#endif /* ZB_STREAM_H */
