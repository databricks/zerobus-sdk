#include <stdlib.h>
#include <string.h>

#include "error.h"
#include "internal/auth/headers_provider.h"
#include "internal/common/utils.h"
#include "internal/platform/log.h"
#include "internal/proto/zerobus_service.h"
#include "state.h"
#include "supervisor.h"
#include "zerobus/error.h"

/* Bytes a reused request may waste beyond the record's own size. */
#define SPARE_SLACK 256u

/* The status and message for an operation refused because the stream is
 * closing or failed. Call with mutex held. A zone closed by neither is a
 * failure being published, so this waits for it. */
static zerobus_status_t refused(zb_ephemeral_stream_t *stream,
                                zerobus_error_t **out_error)
{
    while (stream->failure == ZEROBUS_STATUS_OK && !stream->closing &&
           !stream->aborting) {
        (void)zb_cond_wait(&stream->changed, &stream->mutex);
    }
    if (stream->failure != ZEROBUS_STATUS_OK) {
        return zb_fail(out_error, stream->failure, "stream failed: %s",
                       stream->failure_message ? stream->failure_message
                                               : "unknown error");
    }
    return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                   "stream is closing or closed");
}

static void destroy(zb_ephemeral_stream_t *stream)
{
    zb_landing_zone_free(stream->zone);
    if (stream->spare) {
        while (zb_queue_advance(stream->spare, NULL)) {
        }
        zb_ingest_request_t *request;
        while ((request = zb_queue_pop(stream->spare, NULL))) {
            zb_ingest_request_free(request);
        }
        zb_queue_free(stream->spare);
    }
    if (stream->config.headers) {
        zerobus_headers_provider_free(stream->config.headers);
    }
    free((char *)stream->config.table);
    free((char *)stream->config.host);
    free(stream->failure_message);
    free(stream->close_message);
    (void)zb_cond_destroy(&stream->acked_changed);
    (void)zb_cond_destroy(&stream->changed);
    (void)zb_mutex_destroy(&stream->mutex);
    free(stream);
}

zerobus_status_t
zb_ephemeral_stream_new(const zb_ephemeral_stream_config_t *config,
                        zb_ephemeral_stream_t **out_stream,
                        zerobus_error_t **out_error)
{
    zb_ephemeral_stream_t *stream = calloc(1, sizeof(*stream));
    if (!stream) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status = zb_mutex_init(&stream->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        free(stream);
        return status;
    }
    status = zb_cond_init(&stream->changed);
    if (status == ZEROBUS_STATUS_OK) {
        status = zb_cond_init(&stream->acked_changed);
        if (status != ZEROBUS_STATUS_OK) {
            (void)zb_cond_destroy(&stream->changed);
        }
    }
    if (status != ZEROBUS_STATUS_OK) {
        (void)zb_mutex_destroy(&stream->mutex);
        free(stream);
        return status;
    }
    stream->config = *config;
    stream->config.table = zb_strdup(config->table);
    stream->config.host = zb_strdup(config->host);
    stream->config.headers = zb_headers_provider_ref(config->headers);
    atomic_init(&stream->acked, -1);
    atomic_init(&stream->flush_waiters, 0);
    if (!stream->config.table || !stream->config.host) {
        destroy(stream);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    status = zb_landing_zone_new(config->max_inflight, &stream->zone);
    if (status == ZEROBUS_STATUS_OK) {
        status = zb_queue_new(config->max_inflight, &stream->spare);
    }
    if (status != ZEROBUS_STATUS_OK) {
        destroy(stream);
        return zb_fail(out_error, status,
                       "allocating the landing zone of %zu records failed",
                       config->max_inflight);
    }
    status = zb_thread_create(&stream->supervisor, zb_supervisor_main, stream);
    if (status != ZEROBUS_STATUS_OK) {
        destroy(stream);
        return zb_fail(out_error, status, "starting the supervisor failed");
    }
    stream->supervisor_running = true;

    (void)zb_mutex_lock(&stream->mutex);
    while (!stream->opened) {
        (void)zb_cond_wait(&stream->changed, &stream->mutex);
    }
    status = stream->open_status;
    if (status != ZEROBUS_STATUS_OK) {
        (void)zb_fail(out_error, status, "opening the stream failed: %s",
                      stream->failure_message ? stream->failure_message
                                              : "unknown error");
    }
    (void)zb_mutex_unlock(&stream->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        zb_ephemeral_stream_free(stream);
        return status;
    }
    *out_stream = stream;
    return ZEROBUS_STATUS_OK;
}

void zb_ephemeral_stream_recycle(zb_ephemeral_stream_t *stream,
                                 zb_ingest_request_t *request)
{
    if (zb_queue_push(stream->spare, request, NULL) != ZEROBUS_STATUS_OK) {
        zb_ingest_request_free(request);
    }
}

/* A request for len bytes: a spare one when it fits len and is at most about
 * twice as large, otherwise a new one, so that buffers follow the sizes
 * ingested rather than all growing to the largest. A spare that does not fit
 * means sizes vary, so its replacement gets headroom. */
static zb_ingest_request_t *take_request(zb_ephemeral_stream_t *stream,
                                         size_t len, size_t records)
{
    zb_ingest_request_t *spare = NULL;
    if (zb_queue_advance(stream->spare, NULL)) {
        spare = zb_queue_pop(stream->spare, NULL);
    }
    if (spare && spare->capacity >= len &&
        spare->capacity - len <= len + SPARE_SLACK) {
        spare->len = len;
        spare->records = records;
        spare->data[len] = '\0';
        return spare;
    }
    size_t capacity = len;
    if (spare) {
        capacity += len / 8;
        zb_ingest_request_free(spare);
    }
    return zb_ingest_request_reserve(len, records, capacity);
}

static zb_ingest_request_t *make_request(zb_ephemeral_stream_t *stream,
                                         const zerobus_string_view_t *records,
                                         size_t count)
{
    size_t len =
        count == 1 ? records[0].len : zb_proto_json_batch_size(records, count);
    zb_ingest_request_t *request =
        count == 1 || len ? take_request(stream, len, count) : NULL;
    if (request == NULL) {
        return NULL;
    }
    if (count == 1) {
        memcpy(request->data, records[0].data, len);
    } else {
        zb_proto_write_json_batch(records, count, (uint8_t *)request->data);
    }
    return request;
}

zerobus_status_t zb_ephemeral_stream_ingest(
    zb_ephemeral_stream_t *stream, const zerobus_string_view_t *records,
    size_t count, zerobus_offset_t *out_offset, zerobus_error_t **out_error)
{
    zb_ingest_request_t *request = make_request(stream, records, count);
    if (!request) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_offset_t offset = -1;
    zerobus_status_t status = zb_landing_zone_push(
        stream->zone, request, ZB_DEADLINE_INFINITE, &offset);
    if (status != ZEROBUS_STATUS_OK) {
        zb_ephemeral_stream_recycle(stream, request);
        if (status != ZEROBUS_STATUS_FAILED_PRECONDITION) {
            return zb_fail(out_error, status, "admitting the record failed");
        }
        (void)zb_mutex_lock(&stream->mutex);
        status = refused(stream, out_error);
        (void)zb_mutex_unlock(&stream->mutex);
        return status;
    }
    if (out_offset) {
        *out_offset = offset;
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_ephemeral_stream_flush(zb_ephemeral_stream_t *stream,
                                           zerobus_error_t **out_error)
{
    zerobus_offset_t target = zb_landing_zone_last_offset(stream->zone);
    zb_deadline_t deadline =
        zb_deadline_after_ms(stream->config.flush_timeout_ms);
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    (void)zb_mutex_lock(&stream->mutex);
    /* Published before reading acked, which the receiver writes before it
     * reads flush_waiters: one of the two sees the other. */
    (void)atomic_fetch_add(&stream->flush_waiters, 1);
    while (atomic_load(&stream->acked) < target) {
        if (stream->failure != ZEROBUS_STATUS_OK) {
            status = refused(stream, out_error);
            break;
        }
        if (zb_cond_wait_until(&stream->acked_changed, &stream->mutex,
                               deadline) == ZEROBUS_STATUS_DEADLINE_EXCEEDED &&
            atomic_load(&stream->acked) < target) {
            status = zb_fail(
                out_error, ZEROBUS_STATUS_DEADLINE_EXCEEDED,
                "flush timed out after %llu ms at offset %lld "
                "of %lld",
                (unsigned long long)stream->config.flush_timeout_ms,
                (long long)atomic_load(&stream->acked), (long long)target);
            break;
        }
    }
    (void)atomic_fetch_sub(&stream->flush_waiters, 1);
    (void)zb_mutex_unlock(&stream->mutex);
    return status;
}

zerobus_status_t zb_ephemeral_stream_acked_offset(zb_ephemeral_stream_t *stream,
                                                  zerobus_offset_t *out_offset,
                                                  zerobus_error_t **out_error)
{
    /* Read before the failure check: acknowledgments received before the
     * failure stay valid, so a caller can still complete them. */
    *out_offset = atomic_load(&stream->acked);
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    (void)zb_mutex_lock(&stream->mutex);
    if (stream->failure != ZEROBUS_STATUS_OK) {
        status = refused(stream, out_error);
    }
    (void)zb_mutex_unlock(&stream->mutex);
    return status;
}

/* Joins the supervisor once. Callers are serialized by close or free. */
static void join_supervisor(zb_ephemeral_stream_t *stream)
{
    if (stream->supervisor_running) {
        (void)zb_thread_join(&stream->supervisor, NULL);
        stream->supervisor_running = false;
    }
}

zerobus_status_t zb_ephemeral_stream_close(zb_ephemeral_stream_t *stream,
                                           zerobus_error_t **out_error)
{
    (void)zb_mutex_lock(&stream->mutex);
    if (stream->closing) {
        while (!stream->closed) {
            (void)zb_cond_wait(&stream->changed, &stream->mutex);
        }
        zerobus_status_t status = stream->close_status;
        if (status != ZEROBUS_STATUS_OK) {
            (void)zb_fail(out_error, status, "%s",
                          stream->close_message ? stream->close_message
                                                : "close failed");
        }
        (void)zb_mutex_unlock(&stream->mutex);
        return status;
    }
    stream->closing = true;
    (void)zb_mutex_unlock(&stream->mutex);

    (void)zb_landing_zone_close(stream->zone);
    zerobus_error_t *error = NULL;
    zerobus_status_t status = zb_ephemeral_stream_flush(stream, &error);
    (void)zb_mutex_lock(&stream->mutex);
    if (status == ZEROBUS_STATUS_OK) {
        stream->finishing = true;
    } else {
        stream->aborting = true;
    }
    (void)zb_cond_broadcast(&stream->changed);
    (void)zb_mutex_unlock(&stream->mutex);
    join_supervisor(stream);

    zerobus_string_view_t message = zerobus_error_message(error);
    (void)zb_mutex_lock(&stream->mutex);
    stream->closed = true;
    stream->close_status = status;
    stream->close_message =
        message.data ? zb_strndup(message.data, message.len) : NULL;
    (void)zb_cond_broadcast(&stream->changed);
    (void)zb_mutex_unlock(&stream->mutex);
    if (out_error) {
        *out_error = error;
    } else {
        zerobus_error_free(error);
    }
    ZB_DEBUG("stream to %s closed (status %u)", stream->config.table,
             (unsigned)status);
    return status;
}

void zb_ephemeral_stream_free(zb_ephemeral_stream_t *stream)
{
    if (!stream) {
        return;
    }
    (void)zb_mutex_lock(&stream->mutex);
    stream->aborting = true;
    (void)zb_cond_broadcast(&stream->changed);
    (void)zb_mutex_unlock(&stream->mutex);
    (void)zb_landing_zone_close(stream->zone);
    join_supervisor(stream);
    destroy(stream);
}
