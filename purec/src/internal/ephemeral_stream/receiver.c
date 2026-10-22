#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "internal/common/utils.h"
#include "internal/platform/log.h"
#include "internal/proto/zerobus_service.h"
#include "receiver.h"
#include "state.h"

/* How long a receive waits before rechecking stop and timeouts. */
#define POLL_MS 100u
/* After an acknowledgment, how long to let further responses accumulate. */
#define LINGER_US 50u

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
    zb_ephemeral_stream_t *stream = attempt->stream;
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
        zb_ephemeral_stream_recycle(stream, request);
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

/* Handles one response. Records the highest acknowledgment in *up_to, which
 * must not move backwards. */
static zerobus_status_t handle(zb_attempt_t *attempt, zb_receive_event_t *event,
                               int64_t *up_to, zb_deadline_t *closing_until,
                               char **message)
{
    zb_ephemeral_stream_t *stream = attempt->stream;
    zerobus_offset_t offset;
    if (zb_proto_decode_ack(event->data, event->len, &offset)) {
        if (offset < *up_to) {
            *message = zb_strdup("acknowledged offset moved backwards");
            return ZEROBUS_STATUS_INTERNAL;
        }
        *up_to = offset;
        return ZEROBUS_STATUS_OK;
    }
    zb_response_t response;
    zerobus_status_t status =
        zb_proto_decode_response(event->data, event->len, &response);
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
            uint64_t ms = response.payload.close_stream_signal.has_duration
                              ? (uint64_t)response.payload.close_stream_signal
                                        .duration_seconds *
                                    1000u
                              : 0;
            atomic_store(&attempt->paused, true);
            *closing_until = zb_deadline_after_ms(ms);
            ZB_INFO("server closes the stream in %llu ms",
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
                                  const zb_receive_event_t *event, bool *clean,
                                  char **message)
{
    zb_ephemeral_stream_t *stream = attempt->stream;
    (void)zb_mutex_lock(&stream->mutex);
    bool half_closed = attempt->half_close;
    (void)zb_mutex_unlock(&stream->mutex);
    if (event->status == ZEROBUS_STATUS_OK && half_closed) {
        *clean = true;
        return ZEROBUS_STATUS_OK;
    }
    if (event->status == ZEROBUS_STATUS_OK) {
        *message = zb_strdup("server ended the stream");
        return ZEROBUS_STATUS_UNAVAILABLE;
    }
    *message =
        event->has_grpc_status
            ? format_message("server ended the stream with gRPC status %u: %s",
                             (unsigned)event->grpc_status, event->grpc_message)
            : format_message("stream connection failed (HTTP %u, HTTP/2 error "
                             "%u)",
                             (unsigned)event->http_status,
                             (unsigned)event->http2_error);
    return event->status;
}

void *zb_receiver_main(void *arg)
{
    zb_attempt_t *attempt = arg;
    zb_ephemeral_stream_t *stream = attempt->stream;
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    char *message = NULL;
    bool clean = false;
    int64_t acked = -1;
    zb_deadline_t silent_until =
        zb_deadline_after_ms(stream->config.lack_of_ack_timeout_ms);
    zb_deadline_t closing_until = ZB_DEADLINE_INFINITE;
    while (!atomic_load(&attempt->stop)) {
        zb_receive_event_t event;
        status = zb_transport_recv(attempt->call, zb_deadline_after_ms(POLL_MS),
                                   &event);
        if (status == ZEROBUS_STATUS_DEADLINE_EXCEEDED) {
            status = ZEROBUS_STATUS_OK;
            bool pending = acked + 1 < atomic_load(&attempt->sent);
            if (closing_until != ZB_DEADLINE_INFINITE &&
                (!pending || zb_deadline_expired(closing_until))) {
                status = ZEROBUS_STATUS_UNAVAILABLE;
                message = zb_strdup("server is closing the stream");
                break;
            }
            if (pending && zb_deadline_expired(silent_until)) {
                status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
                message = format_message(
                    "no acknowledgment from the server for %llu ms",
                    (unsigned long long)stream->config.lack_of_ack_timeout_ms);
                break;
            }
            continue;
        }
        if (status != ZEROBUS_STATUS_OK) {
            break;
        }
        /* Take every queued response: only the highest cumulative
         * acknowledgment needs applying. */
        int64_t up_to = acked;
        bool ended = false;
        for (;;) {
            if (event.kind == ZB_RECEIVE_ENDED) {
                ended = true;
                status = judge_end(attempt, &event, &clean, &message);
                break;
            }
            status = handle(attempt, &event, &up_to, &closing_until, &message);
            if (status != ZEROBUS_STATUS_OK) {
                break;
            }
            status =
                zb_transport_recv(attempt->call, ZB_DEADLINE_IMMEDIATE, &event);
            if (status == ZEROBUS_STATUS_DEADLINE_EXCEEDED) {
                status = ZEROBUS_STATUS_OK;
                break;
            }
            if (status != ZEROBUS_STATUS_OK) {
                break;
            }
        }
        silent_until =
            zb_deadline_after_ms(stream->config.lack_of_ack_timeout_ms);
        bool advanced = up_to > acked;
        if (advanced) {
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
        if (advanced) {
            /* A server acknowledging often would otherwise wake this thread
             * once per record; let the next few accumulate. */
            zb_sleep_us(LINGER_US);
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
