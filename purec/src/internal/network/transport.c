#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "io.h"
#include "transport.h"

#define DEFAULT_MAX_MESSAGE_BYTES (16u * 1024u * 1024u)
#define DEFAULT_MAX_HEADER_BYTES (64u * 1024u)
#define DEFAULT_QUEUE_MESSAGES 256u

typedef struct zb_transport_request zb_transport_request_t;

struct zb_transport {
    /* The I/O owner, and the thread that runs it. */
    zb_io_t io;
    zb_thread_t thread;
    bool started;

    /* Each call's queue sizes, fixed at open. */
    size_t send_queue_messages;
    size_t receive_queue_messages;

    /* Shared between workers and with the I/O thread, under mutex. changed is
     * broadcast when a request is done, the connection opens, or the I/O
     * thread stops. */
    zb_mutex_t mutex;
    zb_cond_t changed;
    zb_transport_call_t *calls;
    zb_transport_request_t *pending;
    zb_transport_request_t *pending_tail;

    /* The connection's state, also under mutex. */
    zerobus_status_t status;
    bool ready;
    bool stopping;
    bool terminal;
};

struct zb_transport_call {
    zb_transport_t *transport;
    zb_transport_call_t *next;
    zb_grpc_call_t grpc;
    zb_channel_t *tx;
    zb_channel_t *rx;
    zb_rpc_result_t result;
    atomic_bool ended; /* The I/O thread no longer uses the call. */
};

/* A call open or cancel for the I/O thread to run. It lives on the submitting
 * worker's stack until done. */
struct zb_transport_request {
    zb_transport_request_t *next;
    zb_transport_call_t *call;
    const zb_grpc_call_config_t *config; /* NULL means cancel. */
    zb_deadline_t deadline;
    zerobus_status_t status;
    bool processing;
    bool done;
};

/* ---- control requests -------------------------------------------------- */

/* Removes a request the I/O thread has not taken yet. Call with mutex held. */
static void unqueue(zb_transport_t *transport, zb_transport_request_t *request)
{
    zb_transport_request_t *previous = NULL;
    zb_transport_request_t **entry = &transport->pending;
    while (*entry != request) {
        previous = *entry;
        entry = &previous->next;
    }
    *entry = request->next;
    if (transport->pending_tail == request) {
        transport->pending_tail = previous;
    }
}

/* Runs request on the I/O thread and blocks until it is done, so request can
 * live on the caller's stack. request->deadline bounds only the wait for the
 * I/O thread to take it. */
static zerobus_status_t submit(zb_transport_t *transport,
                               zb_transport_request_t *request)
{
    (void)zb_mutex_lock(&transport->mutex);
    if (transport->terminal || (transport->stopping && request->config)) {
        zerobus_status_t status =
            request->config ? (transport->terminal ? transport->status
                                                   : ZEROBUS_STATUS_CANCELLED)
                            : ZEROBUS_STATUS_OK;
        (void)zb_mutex_unlock(&transport->mutex);
        return status;
    }
    if (transport->pending_tail) {
        transport->pending_tail->next = request;
    } else {
        transport->pending = request;
    }
    transport->pending_tail = request;
    zerobus_status_t status = zb_wakeup_signal(transport->io.wakeup);
    zb_deadline_t deadline = request->deadline;
    while (status == ZEROBUS_STATUS_OK && !request->done) {
        status = zb_cond_wait_until(&transport->changed, &transport->mutex,
                                    deadline);
        if (status != ZEROBUS_STATUS_OK && request->processing &&
            !request->done) {
            /* The I/O thread is running the request, which lives on this
             * stack: wait for it to finish. */
            deadline = ZB_DEADLINE_INFINITE;
            status = ZEROBUS_STATUS_OK;
        }
    }
    if (request->done) {
        status = request->status;
    } else {
        unqueue(transport, request);
    }
    (void)zb_mutex_unlock(&transport->mutex);
    return status;
}

/* ---- gRPC callbacks ---------------------------------------------------- */

static zb_message_t *pull_send(void *context, bool *eof)
{
    zb_transport_call_t *call = context;
    zb_message_t *message = NULL;
    zerobus_status_t status = zb_channel_try_pop(call->tx, &message);
    *eof = status == ZEROBUS_STATUS_OK && message == NULL;
    return message;
}

static bool message_received(void *context, zb_message_t *message)
{
    zb_transport_call_t *call = context;
    return zb_channel_try_push(call->rx, message) == ZEROBUS_STATUS_OK;
}

static void complete(void *context, const zb_rpc_result_t *result)
{
    zb_transport_call_t *call = context;
    call->result = *result;
    /* Closing RX publishes the result, so set it first. */
    (void)zb_channel_close(call->tx);
    (void)zb_channel_close(call->rx);
    /* gRPC has detached the call, so this is its last use. */
    atomic_store(&call->ended, true);
}

/* ---- I/O owner hooks --------------------------------------------------- */

/* The I/O thread's service hook: runs the pending requests, then resumes
 * sending on every call. Never holds mutex while calling gRPC. */
static zerobus_status_t service(void *context)
{
    zb_transport_t *transport = context;
    for (;;) {
        (void)zb_mutex_lock(&transport->mutex);
        if (transport->stopping) {
            (void)zb_mutex_unlock(&transport->mutex);
            return ZEROBUS_STATUS_CANCELLED;
        }
        zb_transport_request_t *request = transport->pending;
        if (!request) {
            (void)zb_mutex_unlock(&transport->mutex);
            break;
        }
        unqueue(transport, request);
        request->processing = true;
        (void)zb_mutex_unlock(&transport->mutex);

        zerobus_status_t status = ZEROBUS_STATUS_OK;
        if (!request->config) {
            zb_grpc_call_cancel(&request->call->grpc);
        } else if (zb_deadline_expired(request->deadline)) {
            status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        } else {
            status = zb_grpc_call_open(&transport->io.grpc,
                                       &request->call->grpc, request->config);
        }
        (void)zb_mutex_lock(&transport->mutex);
        request->status = status;
        request->done = true;
        (void)zb_cond_broadcast(&transport->changed);
        (void)zb_mutex_unlock(&transport->mutex);
    }
    zb_grpc_t *grpc = &transport->io.grpc;
    for (zb_grpc_call_t *call = grpc->calls; call; call = call->next) {
        if (call->request_closed) {
            continue;
        }
        zerobus_status_t status = zb_grpc_call_resume_send(call);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
    }
    return ZEROBUS_STATUS_OK;
}

static void on_ready(void *context)
{
    zb_transport_t *transport = context;
    (void)zb_mutex_lock(&transport->mutex);
    transport->ready = true;
    (void)zb_cond_broadcast(&transport->changed);
    (void)zb_mutex_unlock(&transport->mutex);
}

static void on_stopped(void *context, zerobus_status_t status)
{
    zb_transport_t *transport = context;
    (void)zb_mutex_lock(&transport->mutex);
    transport->stopping = true;
    transport->terminal = true;
    transport->status = status;
    /* Every call has completed by now, so pending opens fail and cancels
     * succeed. */
    while (transport->pending) {
        zb_transport_request_t *request = transport->pending;
        unqueue(transport, request);
        request->status = request->config ? status : ZEROBUS_STATUS_OK;
        request->done = true;
    }
    (void)zb_cond_broadcast(&transport->changed);
    (void)zb_mutex_unlock(&transport->mutex);
}

/* ---- connection startup ------------------------------------------------ */

static zerobus_status_t wait_ready(zb_transport_t *transport,
                                   zb_deadline_t deadline)
{
    (void)zb_mutex_lock(&transport->mutex);
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    while (!transport->ready && !transport->terminal) {
        status = zb_cond_wait_until(&transport->changed, &transport->mutex,
                                    deadline);
        if (status != ZEROBUS_STATUS_OK) {
            break;
        }
    }
    /* A connection that opened and then failed still counts as opened. */
    if (transport->ready) {
        status = ZEROBUS_STATUS_OK;
    } else if (transport->terminal) {
        status = transport->status;
    }
    (void)zb_mutex_unlock(&transport->mutex);
    return status;
}

static zerobus_status_t normalize_config(const zb_transport_config_t *source,
                                         zb_transport_config_t *out)
{
    if (!source || !source->host || !source->host[0] ||
        (source->plaintext && source->tls_context) ||
        (!source->plaintext && !source->tls_context)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    *out = *source;
    if (!out->port) {
        out->port = out->plaintext ? 80 : 443;
    }
    if (!out->max_message_bytes) {
        out->max_message_bytes = DEFAULT_MAX_MESSAGE_BYTES;
    }
    if (!out->max_header_bytes) {
        out->max_header_bytes = DEFAULT_MAX_HEADER_BYTES;
    }
    if (!out->send_queue_messages) {
        out->send_queue_messages = DEFAULT_QUEUE_MESSAGES;
    }
    if (!out->receive_queue_messages) {
        out->receive_queue_messages = DEFAULT_QUEUE_MESSAGES;
    }
    if (out->max_message_bytes > UINT32_MAX ||
        out->max_header_bytes > UINT32_MAX ||
        out->max_header_bytes == SIZE_MAX ||
        out->send_queue_messages >= ZB_SEM_VALUE_MAX ||
        out->receive_queue_messages >= ZB_SEM_VALUE_MAX) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_transport_open(const zb_transport_config_t *config,
                                   zb_deadline_t deadline,
                                   zb_transport_t **out_transport)
{
    if (!out_transport) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zb_transport_config_t options;
    zerobus_status_t status = normalize_config(config, &options);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    size_t host_len = strlen(options.host);
    if (host_len > options.max_header_bytes || host_len > SIZE_MAX - 10) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    char *authority = malloc(host_len + 10);
    zb_transport_t *transport = calloc(1, sizeof(*transport));
    if (!authority || !transport) {
        free(authority);
        free(transport);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    (void)snprintf(authority, host_len + 10,
                   strchr(options.host, ':') ? "[%s]:%u" : "%s:%u",
                   options.host, (unsigned)options.port);
    transport->send_queue_messages = options.send_queue_messages;
    transport->receive_queue_messages = options.receive_queue_messages;
    status = zb_mutex_init(&transport->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        free(authority);
        free(transport);
        return status;
    }
    status = zb_cond_init(&transport->changed);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail_mutex;
    }
    const zb_grpc_config_t grpc_config = {
        .authority = zerobus_string_view(authority),
        .tls = !options.plaintext,
        .max_header_bytes = options.max_header_bytes,
        .max_message_bytes = options.max_message_bytes};
    status = zb_io_init(&transport->io, &grpc_config,
                        (zerobus_string_view_t){options.host, host_len},
                        options.port, options.tls_context, deadline);
    free(authority);
    authority = NULL;
    if (status != ZEROBUS_STATUS_OK) {
        (void)zb_cond_destroy(&transport->changed);
        goto fail_mutex;
    }
    if (zb_deadline_expired(deadline)) {
        status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        goto fail;
    }
    transport->io.context = transport;
    transport->io.service = service;
    transport->io.on_ready = on_ready;
    transport->io.on_stopped = on_stopped;
    status = zb_thread_create(&transport->thread, zb_io_run, &transport->io);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }
    transport->started = true;
    status = wait_ready(transport, deadline);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }
    *out_transport = transport;
    return ZEROBUS_STATUS_OK;
fail:
    zb_transport_free(transport);
    return status;
fail_mutex:
    (void)zb_mutex_destroy(&transport->mutex);
    free(authority);
    free(transport);
    return status;
}

/* ---- call operations --------------------------------------------------- */

zerobus_status_t zb_transport_call_open(zb_transport_t *transport,
                                        zerobus_string_view_t path,
                                        const zerobus_headers_t *headers,
                                        zb_deadline_t deadline,
                                        zb_transport_call_t **out_call)
{
    if (!transport || !out_call) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zb_transport_call_t *call = calloc(1, sizeof(*call));
    if (!call) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    call->transport = transport;
    atomic_init(&call->ended, false);
    zerobus_status_t status = zb_channel_new(
        transport->send_queue_messages, transport->io.wakeup, NULL, &call->tx);
    if (status == ZEROBUS_STATUS_OK) {
        status = zb_channel_new(transport->receive_queue_messages, NULL,
                                transport->io.wakeup, &call->rx);
    }
    if (status != ZEROBUS_STATUS_OK) {
        zb_channel_free(call->tx);
        free(call);
        return status;
    }
    (void)zb_mutex_lock(&transport->mutex);
    call->next = transport->calls;
    transport->calls = call;
    (void)zb_mutex_unlock(&transport->mutex);

    const zb_grpc_call_config_t config = {.path = path,
                                          .headers = headers,
                                          .context = call,
                                          .pull_send = pull_send,
                                          .on_message = message_received,
                                          .on_complete = complete};
    zb_transport_request_t request = {
        .call = call, .config = &config, .deadline = deadline};
    status = submit(transport, &request);
    if (status != ZEROBUS_STATUS_OK) {
        /* The call never opened, so the I/O thread does not use it. */
        atomic_store(&call->ended, true);
        zb_transport_call_free(call);
        return status;
    }
    *out_call = call;
    return ZEROBUS_STATUS_OK;
}

zb_channel_t *zb_transport_tx(const zb_transport_call_t *call)
{
    return call ? call->tx : NULL;
}

zb_channel_t *zb_transport_rx(const zb_transport_call_t *call)
{
    return call ? call->rx : NULL;
}

const zb_rpc_result_t *zb_transport_get_result(const zb_transport_call_t *call)
{
    return call ? &call->result : NULL;
}

zerobus_status_t zb_transport_half_close(zb_transport_call_t *call)
{
    return call ? zb_channel_close(call->tx) : ZEROBUS_STATUS_INVALID_ARGUMENT;
}

zerobus_status_t zb_transport_cancel(zb_transport_call_t *call)
{
    if (!call) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (atomic_load(&call->ended)) {
        return ZEROBUS_STATUS_OK;
    }
    zb_transport_request_t request = {.call = call,
                                      .deadline = ZB_DEADLINE_INFINITE};
    return submit(call->transport, &request);
}

void zb_transport_call_free(zb_transport_call_t *call)
{
    if (!call || zb_transport_cancel(call) != ZEROBUS_STATUS_OK) {
        return;
    }
    zb_transport_t *transport = call->transport;
    (void)zb_mutex_lock(&transport->mutex);
    zb_transport_call_t **link = &transport->calls;
    while (*link && *link != call) {
        link = &(*link)->next;
    }
    if (*link) {
        *link = call->next;
    }
    (void)zb_mutex_unlock(&transport->mutex);
    zb_channel_free(call->tx);
    zb_channel_free(call->rx);
    free(call->result.message);
    free(call);
}

/* ---- connection shutdown ----------------------------------------------- */

zerobus_status_t zb_transport_shutdown(zb_transport_t *transport)
{
    if (!transport) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    (void)zb_mutex_lock(&transport->mutex);
    transport->stopping = true;
    zerobus_status_t status = zb_wakeup_signal(transport->io.wakeup);
    (void)zb_mutex_unlock(&transport->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (transport->started) {
        status = zb_thread_join(&transport->thread, NULL);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        transport->started = false;
    }
    return ZEROBUS_STATUS_OK;
}

void zb_transport_free(zb_transport_t *transport)
{
    if (!transport) {
        return;
    }
    /* The I/O thread may still be running, so keep everything it uses. */
    if (zb_transport_shutdown(transport) != ZEROBUS_STATUS_OK) {
        return;
    }
    while (transport->calls) {
        zb_transport_call_t *call = transport->calls;
        zb_transport_call_free(call);
        if (transport->calls == call) {
            return;
        }
    }
    zb_io_destroy(&transport->io);
    (void)zb_cond_destroy(&transport->changed);
    (void)zb_mutex_destroy(&transport->mutex);
    free(transport);
}
