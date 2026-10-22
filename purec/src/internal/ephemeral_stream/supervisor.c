#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "internal/auth/headers_provider.h"
#include "internal/common/utils.h"
#include "internal/platform/log.h"
#include "internal/proto/zerobus_service.h"
#include "receiver.h"
#include "sender.h"
#include "state.h"
#include "supervisor.h"
#include "zerobus/error.h"

#define EPHEMERAL_STREAM_PATH "/databricks.zerobus.Zerobus/EphemeralStream"
#define USER_AGENT "zerobus-sdk-c/0.0.1"
/* After the half-close, how long the server may take to end the call. */
#define FINISH_DRAIN_MS 2000u
/* After the sender fails, how long the receiver may take to report why the
 * call ended: a call that ends closes the sender's queue first. */
#define END_REPORT_MS 1000u
/* Chunks of encoded records queued between the sender and the I/O
 * thread; see sender.c. */
#define SEND_QUEUE_MESSAGES 16u

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

/* Takes the message out of an error, freeing the error. */
static char *take_message(zerobus_error_t *error)
{
    zerobus_string_view_t view = zerobus_error_message(error);
    char *message = view.data ? zb_strndup(view.data, view.len) : NULL;
    zerobus_error_free(error);
    return message;
}

static bool retryable(zerobus_status_t status)
{
    switch (status) {
    case ZEROBUS_STATUS_INVALID_ARGUMENT:
    case ZEROBUS_STATUS_FAILED_PRECONDITION:
    case ZEROBUS_STATUS_UNAUTHENTICATED:
    case ZEROBUS_STATUS_PERMISSION_DENIED:
    case ZEROBUS_STATUS_NOT_FOUND:
    case ZEROBUS_STATUS_UNIMPLEMENTED:
    case ZEROBUS_STATUS_OUT_OF_MEMORY:
        return false;
    default:
        return true;
    }
}

static void publish_open(zb_ephemeral_stream_t *stream, zerobus_status_t status,
                         const char *message)
{
    (void)zb_mutex_lock(&stream->mutex);
    if (stream->opened) {
        (void)zb_mutex_unlock(&stream->mutex);
        return;
    }
    stream->opened = true;
    stream->open_status = status;
    if (status != ZEROBUS_STATUS_OK && !stream->failure_message && message) {
        stream->failure_message = zb_strdup(message);
    }
    (void)zb_cond_broadcast(&stream->changed);
    (void)zb_mutex_unlock(&stream->mutex);
}

/* Fails the stream for good: later ingests and flushes report status, and
 * producers waiting for space are released. Takes message. */
static void fail_stream(zb_ephemeral_stream_t *stream, zerobus_status_t status,
                        char *message)
{
    /* Close admission first: once a caller sees the failure, no later
     * ingest is admitted. Refused ingests wait for the failure below. */
    (void)zb_landing_zone_close(stream->zone);
    (void)zb_mutex_lock(&stream->mutex);
    if (stream->failure == ZEROBUS_STATUS_OK) {
        stream->failure = status;
        free(stream->failure_message);
        stream->failure_message = message;
        message = NULL;
    }
    ZB_ERROR("stream to %s failed: %s", stream->config.table,
             stream->failure_message ? stream->failure_message : "");
    (void)zb_cond_broadcast(&stream->changed);
    (void)zb_cond_broadcast(&stream->acked_changed);
    (void)zb_mutex_unlock(&stream->mutex);
    free(message);
}

/* Opens a call on *transport, connecting first if needed, and runs the
 * create-stream handshake. On failure leaves no call and sets *message. */
static zerobus_status_t open_call(zb_ephemeral_stream_t *stream,
                                  zb_transport_t **transport,
                                  zb_attempt_t *attempt, char **message)
{
    const zb_ephemeral_stream_config_t *config = &stream->config;
    zb_deadline_t deadline = zb_deadline_after_ms(config->recovery_timeout_ms);
    zerobus_string_view_t table = zerobus_string_view(config->table);
    zb_headers_t headers = {0};
    zb_metadata_t *metadata = NULL;
    zerobus_error_t *error = NULL;
    zerobus_status_t status = zb_headers_provider_get(
        config->headers, table, deadline, &headers, &error);
    if (status != ZEROBUS_STATUS_OK) {
        *message = error ? take_message(error)
                         : format_message("resolving headers failed (%u)",
                                          (unsigned)status);
        return status;
    }
    if (!*transport) {
        const zb_transport_config_t transport_config = {
            .host = config->host,
            .port = config->port,
            .plaintext = config->plaintext,
            .tls_context = config->tls_context,
            .send_queue_messages = SEND_QUEUE_MESSAGES};
        status = zb_transport_open(&transport_config, deadline, transport);
        if (status != ZEROBUS_STATUS_OK) {
            *message =
                format_message("connecting to %s:%u failed (%u)", config->host,
                               (unsigned)config->port, (unsigned)status);
            goto done;
        }
    }
    metadata = malloc((headers.count + 2) * sizeof(*metadata));
    if (!metadata) {
        status = ZEROBUS_STATUS_OUT_OF_MEMORY;
        goto done;
    }
    if (headers.count) {
        memcpy(metadata, headers.items, headers.count * sizeof(*metadata));
    }
    metadata[headers.count] = (zb_metadata_t){
        zerobus_string_view("x-databricks-zerobus-table-name"), table};
    metadata[headers.count + 1] = (zb_metadata_t){
        zerobus_string_view("user-agent"), zerobus_string_view(USER_AGENT)};
    status = zb_transport_call_open(
        *transport, zerobus_string_view(EPHEMERAL_STREAM_PATH), metadata,
        headers.count + 2, deadline, &attempt->call);
    if (status != ZEROBUS_STATUS_OK) {
        *message = format_message("opening the stream call failed (%u)",
                                  (unsigned)status);
        goto done;
    }
    uint8_t *data = NULL;
    size_t len = 0;
    status = zb_proto_encode_create_stream(table, &data, &len);
    if (status == ZEROBUS_STATUS_OK) {
        status = zb_transport_send(attempt->call, data, len, deadline);
        if (status != ZEROBUS_STATUS_OK) {
            free(data);
        }
    }
    zb_receive_event_t event = {0};
    if (status == ZEROBUS_STATUS_OK ||
        status == ZEROBUS_STATUS_FAILED_PRECONDITION) {
        /* A send refused because the call already ended: its end says why. */
        status = zb_transport_recv(attempt->call, deadline, &event);
    }
    if (status != ZEROBUS_STATUS_OK) {
        *message = status == ZEROBUS_STATUS_DEADLINE_EXCEEDED
                       ? format_message(
                             "stream creation timed out after %llu ms",
                             (unsigned long long)config->recovery_timeout_ms)
                       : format_message("stream creation failed (%u)",
                                        (unsigned)status);
        goto done;
    }
    if (event.kind == ZB_RECEIVE_ENDED) {
        status = event.status == ZEROBUS_STATUS_OK ? ZEROBUS_STATUS_INTERNAL
                                                   : event.status;
        *message = event.has_grpc_status
                       ? format_message("server refused to create the stream "
                                        "(gRPC status %u): %s",
                                        (unsigned)event.grpc_status,
                                        event.grpc_message)
                       : format_message("stream connection failed during "
                                        "creation (HTTP %u, HTTP/2 error %u)",
                                        (unsigned)event.http_status,
                                        (unsigned)event.http2_error);
        goto done;
    }
    zb_response_t response;
    status = zb_proto_decode_response(event.data, event.len, &response);
    if (status != ZEROBUS_STATUS_OK ||
        response.kind != ZB_RESPONSE_CREATE_STREAM ||
        !response.payload.create_stream.stream_id) {
        if (status == ZEROBUS_STATUS_OK) {
            zb_response_clear(&response);
        }
        status = ZEROBUS_STATUS_INTERNAL;
        *message = zb_strdup("server answered stream creation unexpectedly");
        goto done;
    }
    ZB_INFO("stream %s created for %s",
            response.payload.create_stream.stream_id, config->table);
    zb_response_clear(&response);
done:
    free(metadata);
    zb_headers_clear(&headers);
    if (status != ZEROBUS_STATUS_OK && attempt->call) {
        zb_transport_call_free(attempt->call);
        attempt->call = NULL;
    }
    return status;
}

/* Runs the attempt's sender and receiver until the call ends, a worker
 * fails, or the stream finishes or aborts, then stops and joins both and
 * frees the call. OK only for a clean finish. */
static zerobus_status_t run_attempt(zb_ephemeral_stream_t *stream,
                                    zb_attempt_t *attempt, char **message)
{
    zb_thread_t sender, receiver;
    attempt->stream = stream;
    (void)zb_landing_zone_reset_observe(stream->zone);
    zerobus_status_t status =
        zb_thread_create(&sender, zb_sender_main, attempt);
    if (status != ZEROBUS_STATUS_OK) {
        *message = zb_strdup("starting the sender failed");
        zb_transport_call_free(attempt->call);
        attempt->call = NULL;
        return status;
    }
    status = zb_thread_create(&receiver, zb_receiver_main, attempt);
    bool receiving = status == ZEROBUS_STATUS_OK;
    if (!receiving) {
        *message = zb_strdup("starting the receiver failed");
    }

    (void)zb_mutex_lock(&stream->mutex);
    zb_deadline_t finish_until = ZB_DEADLINE_INFINITE;
    zb_deadline_t report_until = ZB_DEADLINE_INFINITE;
    while (receiving && !attempt->receiver_done && !stream->aborting) {
        if (stream->finishing && !attempt->half_close) {
            attempt->half_close = true;
            finish_until = zb_deadline_after_ms(FINISH_DRAIN_MS);
            (void)zb_cond_broadcast(&stream->changed);
        }
        if (attempt->sender_done &&
            attempt->sender_status != ZEROBUS_STATUS_OK &&
            report_until == ZB_DEADLINE_INFINITE) {
            report_until = zb_deadline_after_ms(END_REPORT_MS);
        }
        zb_deadline_t until =
            finish_until < report_until ? finish_until : report_until;
        if (zb_cond_wait_until(&stream->changed, &stream->mutex, until) ==
                ZEROBUS_STATUS_DEADLINE_EXCEEDED &&
            !attempt->receiver_done && zb_deadline_expired(until)) {
            break;
        }
    }
    bool clean = attempt->receiver_done && attempt->clean;
    if (!receiving) {
        /* status and message are set. */
    } else if (clean) {
        status = ZEROBUS_STATUS_OK;
    } else if (attempt->receiver_done &&
               attempt->receiver_status != ZEROBUS_STATUS_OK) {
        status = attempt->receiver_status;
        *message = attempt->receiver_message;
        attempt->receiver_message = NULL;
    } else if (attempt->sender_done &&
               attempt->sender_status != ZEROBUS_STATUS_OK) {
        status = attempt->sender_status;
        *message = format_message("sending failed (%u)", (unsigned)status);
    } else if (stream->aborting) {
        status = ZEROBUS_STATUS_CANCELLED;
        *message = zb_strdup("stream was aborted");
    } else {
        status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        *message = zb_strdup("server did not end the stream after close");
    }
    atomic_store(&attempt->stop, true);
    (void)zb_cond_broadcast(&stream->changed);
    (void)zb_mutex_unlock(&stream->mutex);

    if (!clean) {
        /* Wakes a send or receive blocked on the call. */
        (void)zb_transport_cancel(attempt->call);
    }
    (void)zb_thread_join(&sender, NULL);
    if (receiving) {
        (void)zb_thread_join(&receiver, NULL);
    }
    free(attempt->receiver_message);
    attempt->receiver_message = NULL;
    zb_transport_call_free(attempt->call);
    attempt->call = NULL;
    return status;
}

static void drop_transport(zb_transport_t **transport)
{
    if (*transport) {
        (void)zb_transport_shutdown(*transport);
        zb_transport_free(*transport);
        *transport = NULL;
    }
}

/* Sleeps for the recovery backoff unless the stream aborts first. Returns
 * false on abort. */
static bool back_off(zb_ephemeral_stream_t *stream)
{
    zb_deadline_t until =
        zb_deadline_after_ms(stream->config.recovery_backoff_ms);
    (void)zb_mutex_lock(&stream->mutex);
    while (!stream->aborting &&
           zb_cond_wait_until(&stream->changed, &stream->mutex, until) !=
               ZEROBUS_STATUS_DEADLINE_EXCEEDED) {
    }
    bool aborting = stream->aborting;
    (void)zb_mutex_unlock(&stream->mutex);
    return !aborting;
}

void *zb_supervisor_main(void *arg)
{
    zb_ephemeral_stream_t *stream = arg;
    const zb_ephemeral_stream_config_t *config = &stream->config;
    zb_transport_t *transport = NULL;
    bool opened = false;
    bool auth_retry = config->recovery && config->recovery_retries > 0;
    uint32_t failures = 0;
    for (;;) {
        (void)zb_mutex_lock(&stream->mutex);
        bool stop = stream->aborting || stream->finishing;
        (void)zb_mutex_unlock(&stream->mutex);
        if (stop) {
            break;
        }
        zb_attempt_t attempt = {0};
        atomic_init(&attempt.stop, false);
        atomic_init(&attempt.paused, false);
        atomic_init(&attempt.sent, 0);
        char *message = NULL;
        zerobus_status_t status =
            open_call(stream, &transport, &attempt, &message);
        if (status == ZEROBUS_STATUS_OK) {
            if (!opened) {
                opened = true;
                publish_open(stream, ZEROBUS_STATUS_OK, NULL);
            } else {
                ZB_INFO("stream to %s recovered", config->table);
            }
            failures = 0;
            status = run_attempt(stream, &attempt, &message);
            if (status == ZEROBUS_STATUS_OK) {
                break;
            }
        }
        /* A finishing stream is flushed: nothing is left to deliver. */
        (void)zb_mutex_lock(&stream->mutex);
        bool done = stream->aborting || stream->finishing;
        (void)zb_mutex_unlock(&stream->mutex);
        if (done) {
            free(message);
            break;
        }
        drop_transport(&transport);
        bool auth = status == ZEROBUS_STATUS_UNAUTHENTICATED ||
                    status == ZEROBUS_STATUS_PERMISSION_DENIED;
        if (auth) {
            zb_headers_provider_invalidate(config->headers,
                                           zerobus_string_view(config->table));
        }
        /* The first open may spend one retry on a stale credential. */
        bool retry = config->recovery && failures < config->recovery_retries &&
                     (retryable(status) || (!opened && auth && auth_retry));
        if (!opened && auth) {
            auth_retry = false;
        }
        ZB_WARN("stream to %s: %s (status %u)%s", config->table,
                message ? message : "failed", (unsigned)status,
                retry ? ", retrying" : "");
        if (!retry) {
            if (!opened) {
                publish_open(stream, status, message);
            }
            fail_stream(stream, status, message);
            break;
        }
        free(message);
        ++failures;
        if (!back_off(stream)) {
            break;
        }
    }
    drop_transport(&transport);
    if (!opened) {
        publish_open(stream, ZEROBUS_STATUS_CANCELLED, "stream was aborted");
    }
    return NULL;
}
