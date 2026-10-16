#include <stdlib.h>
#include <string.h>

#include "grpc.h"
#include "grpc_utils.h"
#include "internal/common/utils.h"

/* ---- call completion --------------------------------------------------- */

static void release_send(zb_grpc_call_t *call)
{
    zb_message_t *message = call->sending;
    if (message == NULL) {
        return;
    }
    call->sending = NULL;
    free(message->data);
    free(message);
    call->send_prefix_used = 0;
    call->send_payload_used = 0;
}

/* The open call on stream id, or NULL. nghttp2 keeps it as the stream's user
 * data until the call completes. */
static zb_grpc_call_t *find_call(const zb_grpc_t *grpc, int32_t id)
{
    return nghttp2_session_get_stream_user_data(grpc->session, id);
}

/* Like find_call, for a stream whose request HEADERS were never sent, which
 * nghttp2 cannot look up: scans the open calls. */
static zb_grpc_call_t *find_unsent_call(const zb_grpc_t *grpc, int32_t id)
{
    for (zb_grpc_call_t *call = grpc->calls; call != NULL; call = call->next) {
        if (call->id == id) {
            return call;
        }
    }
    return NULL;
}

/* Finishes an open call exactly once: detaches it from the session, frees its
 * owned messages, then passes the result and ownership of grpc_message to
 * on_complete. The call is not touched afterwards, so the owner may free it. */
static void complete(zb_grpc_call_t *call, zerobus_status_t status,
                     bool has_grpc_status, uint32_t http2_error)
{
    zb_grpc_t *grpc = call->grpc;
    if (call->completed) {
        return;
    }
    call->completed = true;
    for (zb_grpc_call_t **link = &grpc->calls; *link != NULL;
         link = &(*link)->next) {
        if (*link == call) {
            *link = call->next;
            break;
        }
    }
    call->next = NULL;
    (void)nghttp2_session_set_stream_user_data(grpc->session, call->id, NULL);
    if (grpc->paused_call == call) {
        grpc->paused_call = NULL;
    }
    release_send(call);
    zb_grpc_decoder_clear(&call->decoder);
    const zb_rpc_result_t result = {.status = status,
                                    .has_grpc_status = has_grpc_status,
                                    .grpc_status = call->grpc_status,
                                    .http_status = call->http_status,
                                    .http2_error = http2_error,
                                    .message = call->grpc_message};
    call->grpc_message = NULL;
    if (call->config.on_complete != NULL) {
        call->config.on_complete(call->config.context, &result);
    } else {
        free(result.message);
    }
}

/* Queues RST_STREAM with error for stream id, which also tells the peer to stop
 * sending. */
static int reset_stream(zb_grpc_t *grpc, int32_t id, uint32_t error)
{
    return nghttp2_submit_rst_stream(grpc->session, NGHTTP2_FLAG_NONE, id,
                                     error);
}

#define CALLBACK_RESULT(result)                                                \
    ((result) == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE)

static int close_call(zb_grpc_call_t *call, zerobus_status_t status,
                      bool has_grpc_status, uint32_t error)
{
    zb_grpc_t *grpc = call->grpc;
    int32_t id = call->id;
    complete(call, status, has_grpc_status, error);
    return reset_stream(grpc, id, error);
}

/* Completes the call for an invalid response header. The returned
 * TEMPORAL_CALLBACK_FAILURE makes nghttp2 close the stream itself. */
static int bad_header(zb_grpc_call_t *call, zerobus_status_t status)
{
    complete(call, status, false, NGHTTP2_PROTOCOL_ERROR);
    return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
}

static int decode(zb_grpc_call_t *call, const uint8_t *data, size_t len,
                  size_t *consumed)
{
    zb_grpc_t *grpc = call->grpc;
    zerobus_status_t result = zb_grpc_decoder_feed(
        &call->decoder, data, len, grpc->config.max_message_bytes,
        call->config.on_message, call->config.context, consumed, &grpc->paused);
    if (result == ZEROBUS_STATUS_OK) {
        return 0;
    }
    return close_call(call, result, false, NGHTTP2_CANCEL);
}

/* ---- nghttp2 callbacks: outgoing --------------------------------------- */

/* Called from nghttp2_session_mem_send whenever a call's request may send DATA,
 * and again after nghttp2_session_resume_data. Fills buffer with up to length
 * bytes of framed messages from pull_send. */
static ssize_t on_data_source_read(nghttp2_session *session, int32_t stream_id,
                                   uint8_t *buffer, size_t length,
                                   uint32_t *data_flags,
                                   nghttp2_data_source *source, void *user_data)
{
    (void)session;
    (void)source;
    zb_grpc_call_t *call = find_call(user_data, stream_id);
    size_t written = 0;
    if (call == NULL) {
        return NGHTTP2_ERR_DEFERRED;
    }
    for (;;) {
        if (call->sending == NULL) {
            bool eof = false;
            call->sending = call->config.pull_send(call->config.context, &eof);
            if (call->sending == NULL) {
                if (eof) {
                    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
                    call->request_closed = true;
                }
                break;
            }
            if (call->sending->len > UINT32_MAX) {
                complete(call, ZEROBUS_STATUS_RESOURCE_EXHAUSTED, false,
                         NGHTTP2_INTERNAL_ERROR);
                return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
            }
            zb_write_grpc_prefix(call->send_prefix,
                                 (uint32_t)call->sending->len);
        }
        if (written == length) {
            break;
        }
        zb_message_t *message = call->sending;
        zb_copy_bounded(buffer, length, &written, call->send_prefix, 5,
                        &call->send_prefix_used);
        zb_copy_bounded(buffer, length, &written, message->data, message->len,
                        &call->send_payload_used);
        if (call->send_prefix_used == 5 &&
            call->send_payload_used == message->len) {
            release_send(call);
        }
    }
    if (written == 0 && !call->request_closed) {
        return NGHTTP2_ERR_DEFERRED;
    }
    return (ssize_t)written;
}

/* Called from nghttp2_session_mem_send after each frame is sent. For a GOAWAY
 * with an error code, aborts the session and returns a session-fatal error. */
static int on_frame_send(nghttp2_session *session, const nghttp2_frame *frame,
                         void *user_data)
{
    (void)session;
    zb_grpc_t *grpc = user_data;
    if (frame->hd.type == NGHTTP2_GOAWAY &&
        frame->goaway.error_code != NGHTTP2_NO_ERROR) {
        zb_grpc_abort(grpc, ZEROBUS_STATUS_INTERNAL);
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    return 0;
}

/* Called from nghttp2_session_mem_send when a frame could not be sent. If it
 * was a call's request HEADERS, completes that call as UNAVAILABLE. */
static int on_frame_not_send(nghttp2_session *session,
                             const nghttp2_frame *frame, int error_code,
                             void *user_data)
{
    (void)session;
    (void)error_code;
    if (frame->hd.type == NGHTTP2_HEADERS) {
        zb_grpc_call_t *call = find_unsent_call(user_data, frame->hd.stream_id);
        if (call != NULL) {
            complete(call, ZEROBUS_STATUS_UNAVAILABLE, false, 0);
        }
    }
    return 0;
}

/* ---- nghttp2 callbacks: incoming --------------------------------------- */

/* Called from nghttp2_session_mem_recv for each field of a received header
 * frame, before on_frame_recv for it. */
static int on_header(nghttp2_session *session, const nghttp2_frame *frame,
                     const uint8_t *name, size_t name_len, const uint8_t *value,
                     size_t value_len, uint8_t flags, void *user_data)
{
    (void)session;
    (void)flags;
    zb_grpc_call_t *call = find_call(user_data, frame->hd.stream_id);
    if (call == NULL) {
        return 0;
    }
    if (!zb_add_http2_header_bytes(&call->header_bytes, name_len, value_len,
                                   call->grpc->config.max_header_bytes)) {
        return bad_header(call, ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    }
    zerobus_string_view_t key = {(const char *)name, name_len};
    if (zb_view_equals(key, ":status")) {
        if (call->block_http_status != 0 || value_len != 3 ||
            !zb_parse_u32(value, value_len, &call->block_http_status) ||
            call->block_http_status < 100) {
            return bad_header(call, ZEROBUS_STATUS_INTERNAL);
        }
    } else if (zb_view_equals(key, "content-type")) {
        if (call->content_type_seen) {
            return bad_header(call, ZEROBUS_STATUS_INTERNAL);
        }
        call->content_type_seen = true;
        call->content_type_valid = zb_is_grpc_content_type(value, value_len);
    } else if (zb_view_equals(key, "grpc-status")) {
        if (call->grpc_status_seen ||
            !zb_parse_u32(value, value_len, &call->grpc_status)) {
            return bad_header(call, ZEROBUS_STATUS_INTERNAL);
        }
        call->grpc_status_seen = true;
    } else if (zb_view_equals(key, "grpc-message")) {
        if (call->grpc_message != NULL) {
            return bad_header(call, ZEROBUS_STATUS_INTERNAL);
        }
        call->grpc_message = malloc(value_len + 1);
        if (call->grpc_message == NULL) {
            return bad_header(call, ZEROBUS_STATUS_OUT_OF_MEMORY);
        }
        memcpy(call->grpc_message, value, value_len);
        zb_grpc_percent_decode(call->grpc_message, value_len);
    }
    return 0;
}

/* Called from nghttp2_session_mem_recv after each complete frame. */
static int on_frame_recv(nghttp2_session *session, const nghttp2_frame *frame,
                         void *user_data)
{
    (void)session;
    zb_grpc_t *grpc = user_data;
    if (frame->hd.type == NGHTTP2_GOAWAY) {
        /* Completes every affected call even if a reset fails, then returns
         * the first failure. */
        int result = 0;
        zb_grpc_call_t *next = NULL;
        for (zb_grpc_call_t *call = grpc->calls; call != NULL; call = next) {
            int32_t id = call->id;
            next = call->next;
            if (id > frame->goaway.last_stream_id ||
                frame->goaway.error_code != NGHTTP2_NO_ERROR) {
                complete(call, ZEROBUS_STATUS_UNAVAILABLE, false,
                         frame->goaway.error_code);
                int reset = reset_stream(grpc, id, NGHTTP2_CANCEL);
                if (result == 0) {
                    result = reset;
                }
            }
        }
        return CALLBACK_RESULT(result);
    }
    zb_grpc_call_t *call = find_call(grpc, frame->hd.stream_id);
    if (call == NULL) {
        return 0;
    }
    bool end = (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0;
    if (frame->hd.type == NGHTTP2_HEADERS) {
        if (call->http_status == 0) {
            if (call->block_http_status == 0) {
                goto invalid;
            }
            if (call->block_http_status < 200) {
                if (end || call->grpc_status_seen ||
                    call->grpc_message != NULL) {
                    goto invalid;
                }
                goto next_headers;
            }
            call->http_status = call->block_http_status;
            if (!call->content_type_valid) {
                if (call->http_status != 200 && !call->grpc_status_seen) {
                    return CALLBACK_RESULT(
                        close_call(call, zb_map_http_status(call->http_status),
                                   false, NGHTTP2_CANCEL));
                }
                goto invalid;
            }
        } else if (call->block_http_status != 0 || !end ||
                   (call->content_type_seen && !call->content_type_valid)) {
            goto invalid;
        }
        if (!end) {
            if (call->grpc_status_seen || call->grpc_message != NULL) {
                goto invalid;
            }
            goto next_headers;
        }
    } else if (frame->hd.type != NGHTTP2_DATA || !end) {
        return 0;
    }
    if (!zb_grpc_decoder_between_messages(&call->decoder)) {
        goto invalid;
    }
    bool has_status =
        frame->hd.type == NGHTTP2_HEADERS && call->grpc_status_seen;
    zerobus_status_t status = has_status
                                  ? zb_map_grpc_status(call->grpc_status)
                                  : zb_map_http_status(call->http_status);
    if (!call->request_closed) {
        return CALLBACK_RESULT(
            close_call(call, status, has_status, NGHTTP2_NO_ERROR));
    }
    complete(call, status, has_status, NGHTTP2_NO_ERROR);
    return 0;

next_headers:
    call->header_bytes = 0;
    call->block_http_status = 0;
    call->content_type_seen = false;
    call->content_type_valid = false;
    return 0;

invalid:
    return CALLBACK_RESULT(close_call(call, ZEROBUS_STATUS_INTERNAL, false,
                                      NGHTTP2_PROTOCOL_ERROR));
}

/* Called from nghttp2_session_mem_recv for each piece of received DATA frame
 * payload. */
static int on_data_chunk(nghttp2_session *session, uint8_t flags,
                         int32_t stream_id, const uint8_t *data, size_t len,
                         void *user_data)
{
    (void)session;
    (void)flags;
    zb_grpc_t *grpc = user_data;
    zb_grpc_call_t *call = find_call(grpc, stream_id);
    if (call == NULL) {
        return 0;
    }
    if (call->http_status == 0) {
        return CALLBACK_RESULT(close_call(call, ZEROBUS_STATUS_INTERNAL, false,
                                          NGHTTP2_PROTOCOL_ERROR));
    }
    size_t consumed = 0;
    int result = decode(call, data, len, &consumed);
    if (result != 0 || !grpc->paused) {
        return CALLBACK_RESULT(result);
    }
    grpc->paused_call = call;
    grpc->remainder = data + consumed;
    grpc->remainder_len = len - consumed;
    return NGHTTP2_ERR_PAUSE;
}

/* Called when a stream closes for any reason. */
static int on_stream_close(nghttp2_session *session, int32_t stream_id,
                           uint32_t error_code, void *user_data)
{
    (void)session;
    zb_grpc_call_t *call = find_call(user_data, stream_id);
    if (call != NULL) {
        complete(call, zb_map_http2_error(error_code), false, error_code);
    }
    return 0;
}

/* ---- request submission ------------------------------------------------ */

/* Queues a call on an initialized session. Nothing is sent: the call opens on
 * the wire when the owner next calls nghttp2_session_mem_send. nghttp2 copies
 * the request fields, so request is not needed afterwards. */
static zerobus_status_t queue_call(zb_grpc_call_t *call,
                                   const zb_grpc_call_config_t *request)
{
    const zb_grpc_config_t *config = &call->grpc->config;
    const zb_metadata_t protocol[] = {
        {zerobus_string_view(":method"), zerobus_string_view("POST")},
        {zerobus_string_view(":scheme"),
         zerobus_string_view(config->tls ? "https" : "http")},
        {zerobus_string_view(":authority"), config->authority},
        {zerobus_string_view(":path"), request->path},
        {zerobus_string_view("content-type"),
         zerobus_string_view("application/grpc")},
        {zerobus_string_view("te"), zerobus_string_view("trailers")}};
    const size_t protocol_count = sizeof(protocol) / sizeof(protocol[0]);
    const size_t count = protocol_count + request->header_count;
    size_t total = 0;
    for (size_t i = 0; i < count; ++i) {
        const zb_metadata_t *header =
            i < protocol_count ? &protocol[i]
                               : &request->headers[i - protocol_count];
        if (!zb_add_http2_header_bytes(&total, header->name.len,
                                       header->value.len,
                                       config->max_header_bytes)) {
            return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
        }
        if (i >= protocol_count && !zb_is_valid_grpc_metadata(header)) {
            return ZEROBUS_STATUS_INVALID_ARGUMENT;
        }
    }
    nghttp2_nv *nva = malloc(count * sizeof(*nva));
    if (nva == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    for (size_t i = 0; i < count; ++i) {
        const zb_metadata_t *header =
            i < protocol_count ? &protocol[i]
                               : &request->headers[i - protocol_count];
        nva[i] = zb_make_nghttp2_nv(header);
    }
    const nghttp2_data_provider provider = {{0}, on_data_source_read};
    int32_t id = nghttp2_submit_request(call->grpc->session, NULL, nva, count,
                                        &provider, call);
    free(nva);
    if (id < 0) {
        return zb_map_nghttp2_error(id);
    }
    call->id = id;
    return ZEROBUS_STATUS_OK;
}

/* ---- component operations ---------------------------------------------- */

zerobus_status_t zb_grpc_init(zb_grpc_t *grpc, const zb_grpc_config_t *config)
{
    if (config == NULL || grpc == NULL || config->max_header_bytes == 0 ||
        config->max_header_bytes > UINT32_MAX ||
        config->max_message_bytes == 0 ||
        config->max_message_bytes > UINT32_MAX ||
        config->authority.len > config->max_header_bytes ||
        !zb_is_valid_grpc_target(config->authority, true)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }

    zerobus_status_t status = ZEROBUS_STATUS_OUT_OF_MEMORY;
    nghttp2_session_callbacks *callbacks = NULL;
    nghttp2_option *option = NULL;
    const nghttp2_settings_entry settings[] = {
        {NGHTTP2_SETTINGS_ENABLE_PUSH, 0},
        {NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE,
         (uint32_t)config->max_header_bytes}};
    grpc->config = *config;
    grpc->authority = zb_strdup_view(config->authority);
    if (grpc->authority == NULL ||
        nghttp2_session_callbacks_new(&callbacks) != 0 ||
        nghttp2_option_new(&option) != 0) {
        goto cleanup;
    }
    grpc->config.authority =
        (zerobus_string_view_t){grpc->authority, config->authority.len};

    nghttp2_session_callbacks_set_on_stream_close_callback(callbacks,
                                                           on_stream_close);
    nghttp2_session_callbacks_set_on_frame_not_send_callback(callbacks,
                                                             on_frame_not_send);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks,
                                                         on_frame_recv);
    nghttp2_session_callbacks_set_on_frame_send_callback(callbacks,
                                                         on_frame_send);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks,
                                                              on_data_chunk);
    nghttp2_option_set_no_closed_streams(option, 1);
    nghttp2_option_set_max_reserved_remote_streams(option, 0);
    nghttp2_option_set_max_send_header_block_length(option,
                                                    config->max_header_bytes);
    if (nghttp2_session_client_new2(&grpc->session, callbacks, grpc, option) ==
            0 &&
        nghttp2_submit_settings(grpc->session, NGHTTP2_FLAG_NONE, settings,
                                sizeof(settings) / sizeof(settings[0])) == 0) {
        status = ZEROBUS_STATUS_OK;
    }
cleanup:
    nghttp2_option_del(option);
    nghttp2_session_callbacks_del(callbacks);
    if (status != ZEROBUS_STATUS_OK) {
        zb_grpc_destroy(grpc);
    }
    return status;
}

void zb_grpc_destroy(zb_grpc_t *grpc)
{
    if (grpc != NULL) {
        zb_grpc_abort(grpc, ZEROBUS_STATUS_CANCELLED);
        nghttp2_session_del(grpc->session);
        free(grpc->authority);
        *grpc = (zb_grpc_t){0};
    }
}

void zb_grpc_abort(zb_grpc_t *grpc, zerobus_status_t status)
{
    if (grpc != NULL && grpc->failure == ZEROBUS_STATUS_OK) {
        grpc->failure =
            status == ZEROBUS_STATUS_OK ? ZEROBUS_STATUS_UNAVAILABLE : status;
        while (grpc->calls != NULL) {
            complete(grpc->calls, grpc->failure, false, NGHTTP2_NO_ERROR);
        }
    }
}

zerobus_status_t zb_grpc_retry_receive(zb_grpc_t *grpc)
{
    if (!grpc->paused) {
        return grpc->failure;
    }
    if (grpc->paused_call == NULL) {
        /* The paused call completed: the rest of its chunk is dropped. */
        grpc->paused = false;
        return grpc->failure;
    }
    size_t consumed = 0;
    int result = decode(grpc->paused_call, grpc->remainder, grpc->remainder_len,
                        &consumed);
    if (consumed != 0) {
        grpc->remainder += consumed;
        grpc->remainder_len -= consumed;
    }
    if (!grpc->paused) {
        grpc->paused_call = NULL;
    }
    if (result < 0) {
        zb_grpc_abort(grpc, zb_map_nghttp2_error(result));
    }
    return grpc->failure;
}

zerobus_status_t zb_grpc_call_open(zb_grpc_t *grpc, zb_grpc_call_t *call,
                                   const zb_grpc_call_config_t *config)
{
    if (grpc == NULL || grpc->session == NULL || call == NULL ||
        (call->grpc != NULL && !call->completed) || config == NULL ||
        config->pull_send == NULL || config->on_message == NULL ||
        (config->headers == NULL && config->header_count != 0)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    /* The 6 are the protocol fields queue_call adds ahead of the headers. */
    if (config->header_count > grpc->config.max_header_bytes / 32 ||
        config->header_count > SIZE_MAX / sizeof(nghttp2_nv) - 6 ||
        config->path.len > grpc->config.max_header_bytes) {
        return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    }
    if (!zb_is_valid_grpc_target(config->path, false)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (grpc->failure != ZEROBUS_STATUS_OK) {
        return grpc->failure;
    }
    /* Keeps only the callbacks: the request fields are borrowed. */
    *call = (zb_grpc_call_t){.grpc = grpc,
                             .config = {.context = config->context,
                                        .pull_send = config->pull_send,
                                        .on_message = config->on_message,
                                        .on_complete = config->on_complete}};
    zerobus_status_t status = queue_call(call, config);
    if (status != ZEROBUS_STATUS_OK) {
        *call = (zb_grpc_call_t){0};
        return status;
    }
    call->next = grpc->calls;
    grpc->calls = call;
    return ZEROBUS_STATUS_OK;
}

void zb_grpc_call_cancel(zb_grpc_call_t *call)
{
    if (call != NULL && call->grpc != NULL && !call->completed) {
        zb_grpc_t *grpc = call->grpc;
        int result =
            close_call(call, ZEROBUS_STATUS_CANCELLED, false, NGHTTP2_CANCEL);
        if (result != 0) {
            zb_grpc_abort(grpc, zb_map_nghttp2_error(result));
        }
    }
}
