#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "internal/common/utils.h"
#include "internal/platform/log.h"
#include "internal/proto/zerobus_service.h"
#include "workers.h"

/* How long a receive waits before rechecking stop and timeouts. */
#define POLL_MS 100u

static char *format_message(const char *format, ...)
#if defined(__GNUC__)
    __attribute__((format(printf, 1, 2)))
#endif
    ;

static char *format_message(const char *format, ...)
{
    char buffer[512];
    va_list args;
    va_start(args, format);
    (void)vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    return zb_strdup(buffer);
}

/* Pops the records the server acknowledged up to physical offset up_to, and
 * publishes the last one's logical offset. */
static zerobus_status_t apply_ack(zb_attempt_t *attempt, int64_t *acked,
                                  int64_t up_to, char **message)
{
    zerobus_stream_t *stream = attempt->stream;
    int64_t sent = atomic_load_explicit(&attempt->sent, memory_order_acquire);
    if (up_to < *acked || up_to >= sent) {
        *message = format_message("server acknowledged offset %lld, but only "
                                  "%lld..%lld are outstanding",
                                  (long long)up_to, (long long)*acked,
                                  (long long)sent - 1);
        return ZEROBUS_STATUS_INTERNAL;
    }
    zerobus_offset_t logical = -1;
    for (; *acked < up_to; ++*acked) {
        zb_ingest_request_t *request =
            zb_landing_zone_pop(stream->zone, &logical);
        if (request == NULL) {
            *message = zb_strdup("acknowledged record is missing");
            return ZEROBUS_STATUS_INTERNAL;
        }
        zb_ingest_request_free(request);
    }
    if (logical >= 0) {
        atomic_store(&stream->acked, logical);
        if (atomic_load(&stream->flush_waiters) != 0) {
            (void)zb_mutex_lock(&stream->mutex);
            (void)zb_cond_broadcast(&stream->acked_changed);
            (void)zb_mutex_unlock(&stream->mutex);
        }
    }
    return ZEROBUS_STATUS_OK;
}

/* How long to keep collecting acknowledgments after the server announced it
 * closes the call: its duration, capped by the stream's setting. */
static uint64_t paused_wait_ms(const zerobus_stream_t *stream,
                               const zb_response_t *response)
{
    uint64_t ms = 0;
    if (response->payload.close_stream_signal.has_duration &&
        response->payload.close_stream_signal.duration_seconds >= 0) {
        uint64_t seconds =
            (uint64_t)response->payload.close_stream_signal.duration_seconds;
        int32_t nanos = response->payload.close_stream_signal.duration_nanos;
        ms = seconds > UINT64_MAX / 1000u ? UINT64_MAX : seconds * 1000u;
        if (nanos > 0 && ms < UINT64_MAX) {
            ms += (uint64_t)nanos / 1000000u;
        }
    }
    uint64_t max_wait = stream->config.stream_paused_max_wait_time_ms;
    return ms < max_wait ? ms : max_wait;
}

/* Handles one response. Records the highest acknowledgment in *up_to, which
 * must not move backwards. */
static zerobus_status_t handle(zb_attempt_t *attempt,
                               const zb_message_t *received, int64_t *up_to,
                               zb_deadline_t *closing_until, char **message)
{
    zerobus_stream_t *stream = attempt->stream;
    zb_response_t response;
    zerobus_status_t status =
        zb_proto_decode_response(received->data, received->len, &response);
    if (status != ZEROBUS_STATUS_OK) {
        *message = zb_strdup("server sent a malformed response");
        return ZEROBUS_STATUS_INTERNAL;
    }
    switch (response.kind) {
    case ZB_RESPONSE_INGEST_RECORD:
        if (!response.payload.ingest_record.has_durability_ack_up_to_offset) {
            status = ZEROBUS_STATUS_INTERNAL;
            *message = zb_strdup("acknowledgment without an offset");
        } else if (response.payload.ingest_record.durability_ack_up_to_offset <
                   *up_to) {
            status = ZEROBUS_STATUS_INTERNAL;
            *message = zb_strdup("acknowledged offset moved backwards");
        } else {
            *up_to = response.payload.ingest_record.durability_ack_up_to_offset;
        }
        break;
    case ZB_RESPONSE_CLOSE_STREAM_SIGNAL:
        if (stream->config.recovery) {
            /* Finish what was sent, then recover on a new call. */
            uint64_t ms = paused_wait_ms(stream, &response);
            atomic_store(&attempt->paused, true);
            *closing_until = zb_deadline_after_ms(ms);
            ZB_INFO("server closes the stream; waiting up to %llu ms for "
                    "acknowledgments",
                    (unsigned long long)ms);
        }
        break;
    case ZB_RESPONSE_CREATE_STREAM:
        status = ZEROBUS_STATUS_INTERNAL;
        *message = zb_strdup("server created the stream twice");
        break;
    default:
        break;
    }
    zb_response_clear(&response);
    return status;
}

/* Judges the call's end. Sets *clean for an OK end after our half-close. */
static zerobus_status_t judge_end(zb_attempt_t *attempt,
                                  const zb_rpc_result_t *result, bool *clean,
                                  char **message)
{
    zerobus_stream_t *stream = attempt->stream;
    (void)zb_mutex_lock(&stream->mutex);
    bool half_closed = attempt->half_close;
    (void)zb_mutex_unlock(&stream->mutex);
    if (result->status == ZEROBUS_STATUS_OK && half_closed) {
        *clean = true;
        return ZEROBUS_STATUS_OK;
    }
    if (result->status == ZEROBUS_STATUS_OK) {
        *message = zb_strdup("server ended the stream");
        return ZEROBUS_STATUS_UNAVAILABLE;
    }
    *message =
        result->has_grpc_status
            ? format_message("server ended the stream with gRPC status %u: %s",
                             (unsigned)result->grpc_status,
                             result->message ? result->message : "")
            : format_message("stream connection failed (HTTP %u, HTTP/2 error "
                             "%u)",
                             (unsigned)result->http_status,
                             (unsigned)result->http2_error);
    return result->status;
}

void *zb_receiver_main(void *arg)
{
    zb_attempt_t *attempt = arg;
    zerobus_stream_t *stream = attempt->stream;
    zb_channel_t *rx = zb_transport_rx(attempt->call);
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    char *message = NULL;
    bool clean = false;
    int64_t acked = -1;
    zb_deadline_t silent_until =
        zb_deadline_after_ms(stream->config.server_lack_of_ack_timeout_ms);
    zb_deadline_t closing_until = ZB_DEADLINE_INFINITE;
    while (!atomic_load(&attempt->stop)) {
        zb_message_t *received = NULL;
        status = zb_channel_pop(rx, zb_deadline_after_ms(POLL_MS), &received);
        if (status == ZEROBUS_STATUS_DEADLINE_EXCEEDED) {
            status = ZEROBUS_STATUS_OK;
            bool pending = acked + 1 < atomic_load(&attempt->sent);
            if (pending && zb_deadline_expired(silent_until)) {
                status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
                message = format_message(
                    "no acknowledgment from the server for %llu ms",
                    (unsigned long long)
                        stream->config.server_lack_of_ack_timeout_ms);
                break;
            }
        } else if (status != ZEROBUS_STATUS_OK) {
            break;
        } else {
            /* Take every queued response: only the highest cumulative
             * acknowledgment needs applying. */
            int64_t up_to = acked;
            bool ended = false;
            for (;;) {
                if (received == NULL) {
                    ended = true;
                    status = judge_end(attempt,
                                       zb_transport_get_result(attempt->call),
                                       &clean, &message);
                    break;
                }
                status =
                    handle(attempt, received, &up_to, &closing_until, &message);
                free(received->data);
                free(received);
                if (status != ZEROBUS_STATUS_OK) {
                    break;
                }
                status = zb_channel_try_pop(rx, &received);
                if (status == ZEROBUS_STATUS_RESOURCE_EXHAUSTED) {
                    status = ZEROBUS_STATUS_OK;
                    break;
                }
                if (status != ZEROBUS_STATUS_OK) {
                    break;
                }
            }
            silent_until = zb_deadline_after_ms(
                stream->config.server_lack_of_ack_timeout_ms);
            if (up_to > acked) {
                /* Applied even when the call just ended, so flush sees it. */
                char *ack_message = NULL;
                zerobus_status_t applied =
                    apply_ack(attempt, &acked, up_to, &ack_message);
                if (status == ZEROBUS_STATUS_OK) {
                    status = applied;
                    message = ack_message;
                } else {
                    free(ack_message);
                }
            }
            if (ended || status != ZEROBUS_STATUS_OK) {
                break;
            }
        }
        /* A closing server gets until closing_until to acknowledge what was
         * sent; recovery then resends the rest on a new call. */
        if (closing_until != ZB_DEADLINE_INFINITE &&
            (acked + 1 >= atomic_load(&attempt->sent) ||
             zb_deadline_expired(closing_until))) {
            status = ZEROBUS_STATUS_UNAVAILABLE;
            message = zb_strdup("server is closing the stream");
            break;
        }
    }
    if (atomic_load(&attempt->stop) && !clean && message == NULL) {
        status = ZEROBUS_STATUS_OK;
    }
    (void)zb_mutex_lock(&stream->mutex);
    attempt->receiver_done = true;
    attempt->receiver_status = status;
    attempt->receiver_message = message;
    attempt->clean = clean;
    (void)zb_cond_broadcast(&stream->changed);
    (void)zb_mutex_unlock(&stream->mutex);
    return NULL;
}
