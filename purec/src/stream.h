#ifndef ZB_STREAM_H
#define ZB_STREAM_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

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

#endif /* ZB_STREAM_H */
