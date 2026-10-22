#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "channel.h"
#include "internal/common/queue.h"
#include "io.h"
#include "transport.h"

/* Sent buffers each call keeps for reuse. A power of two. */
#define SPARE_BUFFERS 32u
/* Received messages each call holds for I/O to free. A power of two. */
#define SPENT_MESSAGES 256u

typedef struct zb_transport_request zb_transport_request_t;

struct zb_transport {
    /* I/O drives the connection; all worker/control state stays here. */
    zb_io_t io;
    zb_thread_t thread;
    size_t max_message_bytes;
    size_t send_queue_messages;
    size_t receive_queue_messages;
    zb_mutex_t mutex;
    zb_cond_t changed;
    zb_transport_call_t *calls;
    zb_transport_request_t *pending;
    zb_transport_request_t *pending_tail;
    zerobus_status_t status;
    bool ready;
    bool stopping;
    bool terminal;
    bool started;
};

struct zb_transport_call {
    zb_transport_t *transport;
    zb_transport_call_t *next;
    zb_grpc_call_t grpc;
    zb_channel_t *tx;
    zb_channel_t *rx;
    /* Sent ZB_TRANSPORT_CHUNK buffers: pushed by I/O, taken by the sender. */
    zb_queue_t *spare;
    /* Received messages the receiver is done with, for I/O to free: freeing
     * on the thread that allocates keeps both cheap. A ring with one
     * producer, the receiver, and one consumer, I/O. */
    _Alignas(64) atomic_size_t spent_head;
    _Alignas(64) atomic_size_t spent_tail;
    zb_message_t *spent[SPENT_MESSAGES];
    /* Receiver only: the message lent by the last recv. */
    zb_message_t *lent;
    zb_rpc_result_t result;
    /* I/O only: pull_send found TX empty and deferred the DATA provider. */
    bool tx_deferred;
};

/* A framed send buffer of ZB_TRANSPORT_CHUNK bytes: the message that carries
 * it, then its data. */
typedef struct {
    zb_message_t message;
    uint8_t data[];
} zb_buffer_t;

static zb_buffer_t *buffer_of(uint8_t *data)
{
    return (zb_buffer_t *)(data - offsetof(zb_buffer_t, data));
}

/* Requests live on the submitting worker's stack until acknowledged. */
struct zb_transport_request {
    zb_transport_request_t *next;
    zb_transport_call_t *call;
    const zb_grpc_call_config_t *config; /* NULL means cancel. */
    zb_deadline_t deadline;
    zerobus_status_t status;
    bool processing;
    bool done;
};

/* Called with the control mutex locked, only for a request not yet taken. */
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
            /* The owner borrowed this stack request. Its operation must
             * finish before the caller can release it. */
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

static zb_message_t *pull_send(void *context, bool *eof)
{
    zb_transport_call_t *call = context;
    zb_message_t *message = NULL;
    zerobus_status_t status = zb_channel_try_pop(call->tx, &message);
    *eof = status == ZEROBUS_STATUS_OK && message == NULL;
    call->tx_deferred = status == ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    return message;
}

static void free_message(zb_message_t *message)
{
    free(message->data);
    free(message);
}

/* Frees a message pull_send returned, or keeps its buffer for reuse. */
static void release_send(void *context, zb_message_t *message)
{
    zb_transport_call_t *call = context;
    if (!message->framed) {
        free_message(message);
        return;
    }
    zb_buffer_t *buffer = buffer_of(message->data);
    if (zb_queue_push(call->spare, buffer, NULL) != ZEROBUS_STATUS_OK) {
        free(buffer);
    }
}

/* Receiver: hands a message it is done with to I/O, or frees it when the
 * ring is full. */
static void retire(zb_transport_call_t *call, zb_message_t *message)
{
    size_t tail = atomic_load_explicit(&call->spent_tail, memory_order_relaxed);
    size_t head = atomic_load_explicit(&call->spent_head, memory_order_acquire);
    if (tail - head == SPENT_MESSAGES) {
        free_message(message);
        return;
    }
    call->spent[tail & (SPENT_MESSAGES - 1)] = message;
    atomic_store_explicit(&call->spent_tail, tail + 1, memory_order_release);
}

/* I/O, or the call's owner once I/O no longer references the call: frees up
 * to limit retired messages. */
static void reclaim(zb_transport_call_t *call, size_t limit)
{
    size_t head = atomic_load_explicit(&call->spent_head, memory_order_relaxed);
    size_t tail = atomic_load_explicit(&call->spent_tail, memory_order_acquire);
    size_t stop = tail - head > limit ? head + limit : tail;
    for (size_t i = head; i != stop; ++i) {
        free_message(call->spent[i & (SPENT_MESSAGES - 1)]);
    }
    atomic_store_explicit(&call->spent_head, stop, memory_order_release);
}

static bool message_received(void *context, zb_message_t *message)
{
    zb_transport_call_t *call = context;
    /* One per message matches what the decoder allocates, so the memory
     * just freed serves the next message. */
    reclaim(call, 1);
    return zb_channel_try_push(call->rx, message) == ZEROBUS_STATUS_OK;
}

static void complete(void *context, const zb_rpc_result_t *result)
{
    zb_transport_call_t *call = context;
    zb_transport_t *transport = call->transport;
    (void)zb_mutex_lock(&transport->mutex);
    call->result = *result;
    /* Result publication precedes RX closure. Only I/O produces/closes RX. */
    (void)zb_channel_close(call->tx);
    (void)zb_channel_close(call->rx);
    (void)zb_mutex_unlock(&transport->mutex);
}

/* Runs only on the I/O owner; the control mutex is never held across gRPC. */
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
        int result = nghttp2_session_resume_data(grpc->session, call->id);
        /* A source that was not deferred has nothing to resume. */
        if (result != NGHTTP2_ERR_INVALID_ARGUMENT) {
            if (grpc->failure != ZEROBUS_STATUS_OK) {
                return grpc->failure;
            }
            if (result < 0) {
                return zb_map_nghttp2_error(result);
            }
        }
    }
    return ZEROBUS_STATUS_OK;
}

/* Runs only on the I/O owner before it sleeps. Arms the wakeup of each TX
 * whose provider deferred on an empty queue, while output can be sent, and
 * of the RX that refused a message. A call blocked only by flow control or
 * by a full socket waits for the socket. */
static bool prepare_wait(void *context, bool can_send)
{
    zb_transport_t *transport = context;
    zb_grpc_t *grpc = &transport->io.grpc;
    bool ready = grpc->paused && !grpc->paused_call;
    for (zb_grpc_call_t *g = grpc->calls; g; g = g->next) {
        zb_transport_call_t *call = g->config.context;
        if (can_send && call->tx_deferred &&
            zb_channel_arm_pop_wakeup(call->tx)) {
            ready = true;
        }
        if (grpc->paused && grpc->paused_call == g &&
            zb_channel_arm_push_wakeup(call->rx)) {
            ready = true;
        }
    }
    return ready;
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
    /* All calls are detached before unopened calls/cancels are acknowledged. */
    while (transport->pending) {
        zb_transport_request_t *request = transport->pending;
        unqueue(transport, request);
        request->status = request->config ? status : ZEROBUS_STATUS_OK;
        request->done = true;
    }
    (void)zb_cond_broadcast(&transport->changed);
    (void)zb_mutex_unlock(&transport->mutex);
}

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
    /* Successful TCP/TLS setup remains success if the connection then fails. */
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
        out->max_message_bytes = 16u * 1024u * 1024u;
    }
    if (!out->max_header_bytes) {
        out->max_header_bytes = 64u * 1024u;
    }
    if (!out->send_queue_messages) {
        out->send_queue_messages = 256u;
    }
    if (!out->receive_queue_messages) {
        out->receive_queue_messages = 256u;
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
    transport->max_message_bytes = options.max_message_bytes;
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
    status = zb_io_init(&transport->io, &grpc_config, options.host,
                        options.port, options.tls_context, deadline);
    free(authority);
    authority = NULL;
    if (status != ZEROBUS_STATUS_OK) {
        (void)zb_cond_destroy(&transport->changed);
        goto fail_mutex;
    }
    transport->io.context = transport;
    transport->io.service = service;
    transport->io.prepare_wait = prepare_wait;
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

zerobus_status_t
zb_transport_call_open(zb_transport_t *transport, zerobus_string_view_t path,
                       const zb_metadata_t *headers, size_t header_count,
                       zb_deadline_t deadline, zb_transport_call_t **out_call)
{
    if (!transport || !out_call) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zb_transport_call_t *call = calloc(1, sizeof(*call));
    if (!call) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    call->transport = transport;
    atomic_init(&call->spent_head, 0);
    atomic_init(&call->spent_tail, 0);
    zerobus_status_t status = zb_channel_new(transport->send_queue_messages,
                                             transport->io.wakeup, &call->tx);
    if (status == ZEROBUS_STATUS_OK) {
        status = zb_channel_new(transport->receive_queue_messages,
                                transport->io.wakeup, &call->rx);
    }
    if (status != ZEROBUS_STATUS_OK) {
        zb_channel_free(call->tx);
        free(call);
        return status;
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = zb_queue_new(SPARE_BUFFERS, &call->spare);
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = zb_mutex_lock(&transport->mutex);
    }
    if (status != ZEROBUS_STATUS_OK) {
        zb_queue_free(call->spare);
        zb_channel_free(call->tx);
        zb_channel_free(call->rx);
        free(call);
        return status;
    }
    call->next = transport->calls;
    transport->calls = call;
    (void)zb_mutex_unlock(&transport->mutex);

    const zb_grpc_call_config_t config = {.path = path,
                                          .headers = headers,
                                          .header_count = header_count,
                                          .context = call,
                                          .pull_send = pull_send,
                                          .release_send = release_send,
                                          .on_message = message_received,
                                          .on_complete = complete};
    zb_transport_request_t request = {
        .call = call, .config = &config, .deadline = deadline};
    status = submit(transport, &request);
    if (status != ZEROBUS_STATUS_OK) {
        zb_transport_call_free(call);
        return status;
    }
    *out_call = call;
    return ZEROBUS_STATUS_OK;
}

static zerobus_status_t queue_message(zb_transport_call_t *call, uint8_t *data,
                                      size_t len, bool framed,
                                      zb_deadline_t deadline)
{
    if (!call || (!data && len)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (len > call->transport->max_message_bytes) {
        return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    }
    zb_message_t *message;
    if (framed) {
        message = &buffer_of(data)->message;
    } else if (!(message = malloc(sizeof(*message)))) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    *message = (zb_message_t){.data = data, .len = len, .framed = framed};
    zerobus_status_t status = zb_channel_push(call->tx, message, deadline);
    if (status != ZEROBUS_STATUS_OK && !framed) {
        free(message); /* Failure leaves payload ownership with the caller. */
    }
    return status;
}

zerobus_status_t zb_transport_send(zb_transport_call_t *call, uint8_t *data,
                                   size_t len, zb_deadline_t deadline)
{
    return queue_message(call, data, len, false, deadline);
}

void zb_transport_write_prefix(uint8_t *out, uint32_t len)
{
    zb_write_grpc_prefix(out, len);
}

uint8_t *zb_transport_buffer(zb_transport_call_t *call)
{
    zb_buffer_t *buffer = NULL;
    if (zb_queue_advance(call->spare, NULL)) {
        buffer = zb_queue_pop(call->spare, NULL);
    }
    if (!buffer && !(buffer = malloc(sizeof(*buffer) + ZB_TRANSPORT_CHUNK))) {
        return NULL;
    }
    return buffer->data;
}

void zb_transport_buffer_free(uint8_t *buffer)
{
    if (buffer) {
        free(buffer_of(buffer));
    }
}

zerobus_status_t zb_transport_send_framed(zb_transport_call_t *call,
                                          uint8_t *buffer, size_t len,
                                          zb_deadline_t deadline)
{
    return queue_message(call, buffer, len, true, deadline);
}

zerobus_status_t zb_transport_recv(zb_transport_call_t *call,
                                   zb_deadline_t deadline,
                                   zb_receive_event_t *out_event)
{
    if (!call || !out_event) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (call->lent) {
        retire(call, call->lent);
        call->lent = NULL;
    }
    zb_message_t *message;
    zerobus_status_t status = zb_channel_pop(call->rx, deadline, &message);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (message) {
        call->lent = message;
        *out_event = (zb_receive_event_t){.kind = ZB_RECEIVE_MESSAGE,
                                          .data = message->data,
                                          .len = message->len};
    } else {
        (void)zb_mutex_lock(&call->transport->mutex);
        zb_rpc_result_t result = call->result;
        (void)zb_mutex_unlock(&call->transport->mutex);
        *out_event = (zb_receive_event_t){
            .kind = ZB_RECEIVE_ENDED,
            .status = result.status,
            .has_grpc_status = result.has_grpc_status,
            .grpc_status = result.grpc_status,
            .http_status = result.http_status,
            .http2_error = result.http2_error,
            .grpc_message = result.message ? result.message : ""};
    }
    return ZEROBUS_STATUS_OK;
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
    if (zb_mutex_lock(&transport->mutex) != ZEROBUS_STATUS_OK) {
        return;
    }
    zb_transport_call_t **link = &transport->calls;
    while (*link && *link != call) {
        link = &(*link)->next;
    }
    if (*link) {
        *link = call->next;
    }
    (void)zb_mutex_unlock(&transport->mutex);
    /* Framed messages live inside their buffers, which the channel cannot
     * free. */
    zb_message_t *message;
    while (zb_channel_try_pop(call->tx, &message) == ZEROBUS_STATUS_OK &&
           message) {
        release_send(call, message);
    }
    zb_channel_free(call->tx);
    zb_channel_free(call->rx);
    if (call->lent) {
        free_message(call->lent);
    }
    reclaim(call, SPENT_MESSAGES);
    zb_buffer_t *buffer;
    while (zb_queue_advance(call->spare, NULL)) {
    }
    while ((buffer = zb_queue_pop(call->spare, NULL))) {
        free(buffer);
    }
    zb_queue_free(call->spare);
    free(call->result.message);
    free(call);
}

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
    /* Keep owner state alive on an unexpected wake/join failure. */
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
