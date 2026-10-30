#include <stdlib.h>

#include "internal/platform/log.h"
#include "internal/proto/zerobus_service.h"
#include "workers.h"

/* How long an idle sender waits for a record before rechecking stop. */
#define IDLE_POLL_MS 50u

/* Waits until the supervisor asks for the half-close or for a stop. Returns
 * true for the half-close. */
static bool await_half_close(zb_attempt_t *attempt)
{
    zerobus_stream_t *stream = attempt->stream;
    (void)zb_mutex_lock(&stream->mutex);
    while (!attempt->half_close && !atomic_load(&attempt->stop)) {
        (void)zb_cond_wait(&stream->changed, &stream->mutex);
    }
    bool half_close = attempt->half_close && !atomic_load(&attempt->stop);
    (void)zb_mutex_unlock(&stream->mutex);
    return half_close;
}

/* Encodes the request at physical and queues it on the call, waiting for
 * room. One record goes out as ingest_record and more as one batch, as in the
 * Rust SDK. Publishes the offset first, as an acknowledgment may follow at
 * once. */
static zerobus_status_t send_request(zb_attempt_t *attempt,
                                     const zb_ingest_request_t *request,
                                     int64_t physical)
{
    zb_message_t *message = malloc(sizeof(*message));
    if (!message) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status =
        request->count == 1
            ? zb_proto_encode_ingest_record(
                  physical,
                  (zerobus_string_view_t){(const char *)request->batch[0].data,
                                          request->batch[0].len},
                  &message->data, &message->len)
            : zb_proto_encode_json_batch(physical, request, &message->data,
                                         &message->len);
    if (status != ZEROBUS_STATUS_OK) {
        free(message);
        return status;
    }
    atomic_store_explicit(&attempt->sent, physical + 1, memory_order_release);
    status = zb_channel_push(zb_transport_tx(attempt->call), message,
                             ZB_DEADLINE_INFINITE);
    if (status != ZEROBUS_STATUS_OK) {
        free(message->data);
        free(message);
    }
    return status;
}

void *zb_sender_main(void *arg)
{
    zb_attempt_t *attempt = arg;
    zerobus_stream_t *stream = attempt->stream;
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    int64_t physical = 0;
    while (!atomic_load_explicit(&attempt->stop, memory_order_relaxed)) {
        if (atomic_load_explicit(&attempt->paused, memory_order_relaxed)) {
            /* The server is closing the call: wait for the supervisor. */
            (void)await_half_close(attempt);
            break;
        }
        zb_ingest_request_t *request = NULL;
        zerobus_offset_t logical = -1;
        status = zb_landing_zone_observe(stream->zone,
                                         zb_deadline_after_ms(IDLE_POLL_MS),
                                         &request, &logical);
        if (status == ZEROBUS_STATUS_DEADLINE_EXCEEDED) {
            status = ZEROBUS_STATUS_OK;
            continue;
        }
        if (status == ZEROBUS_STATUS_RESOURCE_EXHAUSTED) {
            /* Closed and drained. Half-close only when asked: the server may
             * end the call without acknowledging records still in flight. */
            status = ZEROBUS_STATUS_OK;
            if (await_half_close(attempt)) {
                status = zb_transport_half_close(attempt->call);
                ZB_DEBUG("half-closed after %lld records", (long long)physical);
            }
            break;
        }
        if (status != ZEROBUS_STATUS_OK) {
            break;
        }
        status = send_request(attempt, request, physical++);
        if (status != ZEROBUS_STATUS_OK) {
            ZB_DEBUG("sending record %lld failed (%u)", (long long)logical,
                     (unsigned)status);
            break;
        }
    }
    if (atomic_load(&attempt->stop)) {
        status = ZEROBUS_STATUS_OK;
    }
    (void)zb_mutex_lock(&stream->mutex);
    attempt->sender_done = true;
    attempt->sender_status = status;
    (void)zb_cond_broadcast(&stream->changed);
    (void)zb_mutex_unlock(&stream->mutex);
    return NULL;
}
