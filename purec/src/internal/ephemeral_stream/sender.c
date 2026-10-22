#include <string.h>

#include "internal/platform/log.h"
#include "internal/proto/zerobus_service.h"
#include "sender.h"
#include "state.h"

/* How long an idle sender waits for a record before rechecking stop. */
#define IDLE_POLL_MS 50u
/* How long a busy sender that ran out of records sleeps before it looks
 * again, and so the longest a partly filled DATA frame waits for more.
 * Sleeping without waiting on the landing zone spares ingest a wakeup per
 * record. */
#define LINGER_US 50u

/* Framed requests in one transport buffer, sent with one transport call. A
 * request may continue in the next chunk. */
typedef struct {
    uint8_t *data;
    size_t len;
} chunk_t;

/* Waits until the supervisor asks for the half-close or for a stop. Returns
 * true for the half-close. */
static bool await_half_close(zb_attempt_t *attempt)
{
    zb_ephemeral_stream_t *stream = attempt->stream;
    (void)zb_mutex_lock(&stream->mutex);
    while (!attempt->half_close && !atomic_load(&attempt->stop)) {
        (void)zb_cond_wait(&stream->changed, &stream->mutex);
    }
    bool half_close = attempt->half_close && !atomic_load(&attempt->stop);
    (void)zb_mutex_unlock(&stream->mutex);
    return half_close;
}

/* Queues the first len bytes of the chunk on the call, which takes them over
 * on success. The rest moves to the start of a new chunk. */
static zerobus_status_t queue_bytes(zb_attempt_t *attempt, chunk_t *chunk,
                                    size_t len)
{
    if (len == 0) {
        return ZEROBUS_STATUS_OK;
    }
    chunk_t rest = {0};
    if (len < chunk->len) {
        rest.data = zb_transport_buffer(attempt->call);
        if (!rest.data) {
            return ZEROBUS_STATUS_OUT_OF_MEMORY;
        }
        rest.len = chunk->len - len;
        memcpy(rest.data, chunk->data + len, rest.len);
    }
    zerobus_status_t status = zb_transport_send_framed(
        attempt->call, chunk->data, len, ZB_DEADLINE_INFINITE);
    if (status != ZEROBUS_STATUS_OK) {
        zb_transport_buffer_free(chunk->data);
        zb_transport_buffer_free(rest.data);
        rest = (chunk_t){0};
    }
    *chunk = rest;
    return status;
}

/* Copies len bytes into the chunk, sending each chunk that fills. */
static zerobus_status_t put(zb_attempt_t *attempt, chunk_t *chunk,
                            const uint8_t *data, size_t len)
{
    while (len > 0) {
        if (chunk->data && chunk->len == ZB_TRANSPORT_CHUNK) {
            zerobus_status_t status = queue_bytes(attempt, chunk, chunk->len);
            if (status != ZEROBUS_STATUS_OK) {
                return status;
            }
        }
        if (!chunk->data) {
            chunk->data = zb_transport_buffer(attempt->call);
            if (!chunk->data) {
                return ZEROBUS_STATUS_OUT_OF_MEMORY;
            }
            chunk->len = 0;
        }
        size_t room = ZB_TRANSPORT_CHUNK - chunk->len;
        size_t n = len < room ? len : room;
        memcpy(chunk->data + chunk->len, data, n);
        chunk->len += n;
        data += n;
        len -= n;
    }
    return ZEROBUS_STATUS_OK;
}

/* Frames the request at physical into the chunk. */
static zerobus_status_t append(zb_attempt_t *attempt, chunk_t *chunk,
                               const zb_ingest_request_t *request,
                               int64_t physical)
{
    uint8_t header[ZB_TRANSPORT_PREFIX + ZB_PROTO_INGEST_HEADER_MAX];
    size_t len =
        zb_proto_write_ingest_header(header + ZB_TRANSPORT_PREFIX, physical,
                                     request->len, request->records > 1);
    if (len + request->len > UINT32_MAX) {
        return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    }
    zb_transport_write_prefix(header, (uint32_t)(len + request->len));
    zerobus_status_t status =
        put(attempt, chunk, header, ZB_TRANSPORT_PREFIX + len);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    return put(attempt, chunk, (const uint8_t *)request->data, request->len);
}

void *zb_sender_main(void *arg)
{
    zb_attempt_t *attempt = arg;
    zb_ephemeral_stream_t *stream = attempt->stream;
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    chunk_t chunk = {0};
    int64_t physical = 0;
    /* Records were waiting when looked for: more will likely come within
     * LINGER_US. Cleared once a linger finds none. */
    bool busy = false;
    while (!atomic_load_explicit(&attempt->stop, memory_order_relaxed)) {
        if (atomic_load_explicit(&attempt->paused, memory_order_relaxed)) {
            /* The server is closing the call: send what is numbered, then
             * wait for the supervisor. */
            status = queue_bytes(attempt, &chunk, chunk.len);
            (void)await_half_close(attempt);
            break;
        }
        zb_ingest_request_t *request = NULL;
        zerobus_offset_t logical = -1;
        status = zb_landing_zone_observe(stream->zone, ZB_DEADLINE_IMMEDIATE,
                                         &request, &logical);
        if (status == ZEROBUS_STATUS_OK) {
            busy = true;
        }
        if (status == ZEROBUS_STATUS_DEADLINE_EXCEEDED && busy) {
            /* Nothing more right now: send the whole frames at once, and the
             * partial one only if no record comes to fill it. */
            status = queue_bytes(attempt, &chunk,
                                 chunk.len - chunk.len % ZB_TRANSPORT_FRAME);
            if (status != ZEROBUS_STATUS_OK) {
                break;
            }
            zb_sleep_us(LINGER_US);
            status = zb_landing_zone_observe(
                stream->zone, ZB_DEADLINE_IMMEDIATE, &request, &logical);
            busy = status != ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        }
        if (status == ZEROBUS_STATUS_DEADLINE_EXCEEDED) {
            /* Idle: send everything, then wait for the next record. */
            status = queue_bytes(attempt, &chunk, chunk.len);
            if (status != ZEROBUS_STATUS_OK) {
                break;
            }
            status = zb_landing_zone_observe(stream->zone,
                                             zb_deadline_after_ms(IDLE_POLL_MS),
                                             &request, &logical);
            if (status == ZEROBUS_STATUS_DEADLINE_EXCEEDED) {
                status = ZEROBUS_STATUS_OK;
                continue;
            }
        }
        if (status == ZEROBUS_STATUS_RESOURCE_EXHAUSTED) {
            /* Closed and drained. Half-close only when asked: the server may
             * end the call without acknowledging records still in flight. */
            status = queue_bytes(attempt, &chunk, chunk.len);
            if (status == ZEROBUS_STATUS_OK && await_half_close(attempt)) {
                status = zb_transport_half_close(attempt->call);
                ZB_DEBUG("half-closed after %lld records", (long long)physical);
            }
            break;
        }
        if (status != ZEROBUS_STATUS_OK) {
            break;
        }
        status = append(attempt, &chunk, request, physical);
        if (status != ZEROBUS_STATUS_OK) {
            ZB_ERROR("sending record %lld failed", (long long)logical);
            break;
        }
        /* Publish before the chunk goes out: an acknowledgment may follow at
         * once. */
        atomic_store_explicit(&attempt->sent, ++physical, memory_order_release);
    }
    zb_transport_buffer_free(chunk.data);
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
