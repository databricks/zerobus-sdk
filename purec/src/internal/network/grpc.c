#include <stdlib.h>
#include <string.h>

#include "grpc.h"
#include "internal/common/utils.h"

/* ---- message framing --------------------------------------------------- */

static void write_prefix(uint8_t prefix[5], uint32_t length)
{
    prefix[0] = 0; /* uncompressed flag */
    prefix[1] = (uint8_t)(length >> 24);
    prefix[2] = (uint8_t)(length >> 16);
    prefix[3] = (uint8_t)(length >> 8);
    prefix[4] = (uint8_t)length;
}

static void decoder_clear(zb_grpc_decoder_t *decoder)
{
    if (decoder->message != NULL) {
        free(decoder->message->data);
        free(decoder->message);
    }
    memset(decoder, 0, sizeof(*decoder));
}

static bool decoder_between_messages(const zb_grpc_decoder_t *decoder)
{
    return decoder->prefix_used == 0 && decoder->message == NULL;
}

static zerobus_status_t start_message(zb_grpc_decoder_t *decoder,
                                      size_t maximum)
{
    const uint8_t *prefix = decoder->prefix;
    if (prefix[0] != 0) {
        return prefix[0] == 1 ? ZEROBUS_STATUS_UNIMPLEMENTED
                              : ZEROBUS_STATUS_INTERNAL;
    }
    uint32_t length = 0;
    for (size_t i = 1; i < 5; ++i) {
        length = (length << 8) | prefix[i];
    }
    if ((size_t)length > maximum) {
        return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    }
    zb_message_t *message = calloc(1, sizeof(*message));
    if (message == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    message->len = length;
    if (length != 0 && (message->data = malloc(length)) == NULL) {
        free(message);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    decoder->message = message;
    return ZEROBUS_STATUS_OK;
}

static void fill_from_input(uint8_t *buffer, size_t size, size_t *used,
                            const uint8_t *data, size_t len, size_t *consumed)
{
    size_t take = size - *used;
    if (take > len - *consumed) {
        take = len - *consumed;
    }
    if (take != 0) {
        memcpy(buffer + *used, data + *consumed, take);
        *used += take;
        *consumed += take;
    }
}

/* Decodes gRPC messages from a stream that arrives in chunks, one chunk per
 * call. A message split across chunks is completed by later calls.
 *
 * deliver is the output: it receives each complete message in order and
 * returns true to take ownership of it. It returns false to refuse the message
 * for now (e.g. no room downstream): the decoder keeps it, stops right after
 * it, and sets *paused. While paused, the next call, made with the rest of the
 * same input, offers the kept message again. */
static zerobus_status_t
decoder_feed(zb_grpc_decoder_t *decoder, const uint8_t *data, size_t len,
             size_t maximum, bool (*deliver)(void *, zb_message_t *),
             void *context, size_t *consumed, bool *paused)
{
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    *consumed = 0;
    *paused = false;
    while (status == ZEROBUS_STATUS_OK) {
        zb_message_t *message = decoder->message;
        if (message != NULL && decoder->payload_used == message->len) {
            if (!deliver(context, message)) {
                *paused = true;
                break;
            }
            *decoder = (zb_grpc_decoder_t){0};
        } else if (*consumed == len) {
            break;
        } else if (message == NULL) {
            fill_from_input(decoder->prefix, 5, &decoder->prefix_used, data,
                            len, consumed);
            if (decoder->prefix_used == 5) {
                status = start_message(decoder, maximum);
            }
        } else {
            fill_from_input(message->data, message->len, &decoder->payload_used,
                            data, len, consumed);
        }
    }
    return status;
}

/* ---- status mapping ---------------------------------------------------- */

static zerobus_status_t map_grpc_status(uint32_t code)
{
    switch (code) {
    case 0:
        return ZEROBUS_STATUS_OK;
    case 1:
        return ZEROBUS_STATUS_CANCELLED;
    case 3:
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    case 4:
        return ZEROBUS_STATUS_DEADLINE_EXCEEDED;
    case 5:
        return ZEROBUS_STATUS_NOT_FOUND;
    case 7:
        return ZEROBUS_STATUS_PERMISSION_DENIED;
    case 8:
        return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    case 9:
        return ZEROBUS_STATUS_FAILED_PRECONDITION;
    case 12:
        return ZEROBUS_STATUS_UNIMPLEMENTED;
    case 13:
        return ZEROBUS_STATUS_INTERNAL;
    case 14:
        return ZEROBUS_STATUS_UNAVAILABLE;
    case 16:
        return ZEROBUS_STATUS_UNAUTHENTICATED;
    default:
        return ZEROBUS_STATUS_UNKNOWN;
    }
}

static zerobus_status_t map_http_status(uint32_t code)
{
    switch (code) {
    case 400:
        return ZEROBUS_STATUS_INTERNAL;
    case 401:
        return ZEROBUS_STATUS_UNAUTHENTICATED;
    case 403:
        return ZEROBUS_STATUS_PERMISSION_DENIED;
    case 404:
        return ZEROBUS_STATUS_UNIMPLEMENTED;
    case 429:
    case 502:
    case 503:
    case 504:
        return ZEROBUS_STATUS_UNAVAILABLE;
    default:
        return ZEROBUS_STATUS_UNKNOWN;
    }
}

static zerobus_status_t map_http2_error(uint32_t error)
{
    switch (error) {
    case NGHTTP2_REFUSED_STREAM:
        return ZEROBUS_STATUS_UNAVAILABLE;
    case NGHTTP2_CANCEL:
        return ZEROBUS_STATUS_CANCELLED;
    case NGHTTP2_ENHANCE_YOUR_CALM:
        return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    case NGHTTP2_INADEQUATE_SECURITY:
        return ZEROBUS_STATUS_PERMISSION_DENIED;
    default:
        return ZEROBUS_STATUS_INTERNAL;
    }
}

/* ---- header values ----------------------------------------------------- */

static bool parse_decimal(const uint8_t *value, size_t len, uint32_t *out)
{
    if (len == 0) {
        return false;
    }
    uint32_t res = 0;
    for (size_t i = 0; i < len; ++i) {
        uint32_t c = (uint32_t)(value[i] - '0');
        if (value[i] < '0' || value[i] > '9' || res > (UINT32_MAX - c) / 10) {
            return false;
        }
        res = res * 10 + c;
    }
    *out = res;
    return true;
}

/* True for application/grpc, matched case-insensitively and optionally followed
 * by a "+subtype" or ";parameters". */
static bool is_grpc_content_type(const uint8_t *value, size_t len)
{
    static const char type[] = "application/grpc";
    const size_t prefix = sizeof(type) - 1;
    return zb_starts_with((const char *)value, len, type) &&
           (len == prefix || value[prefix] == '+' || value[prefix] == ';');
}

static int hex(char c)
{
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

/* Percent-decodes a grpc-message value in place and NUL-terminates it, so value
 * needs len + 1 bytes. Malformed escapes remain literal as required by gRPC. */
static void percent_decode(char *value, size_t len)
{
    size_t out = 0;
    for (size_t i = 0; i < len; ++i) {
        char c = value[i];
        if (c == '%' && len - i >= 3) {
            int high = hex(value[i + 1]);
            int low = hex(value[i + 2]);
            if (high >= 0 && low >= 0) {
                c = (char)(high * 16 + low);
                i += 2;
            }
        }
        value[out++] = c == '\0' ? '?' : c;
    }
    value[out] = '\0';
}

/* Adds one header field's size to *total, counted the way HTTP/2 sizes header
 * lists (RFC 7540, 6.5.2): name + value + 32 octets of per-field overhead.
 * Returns false, leaving *total unchanged, when that would exceed limit. */
static bool add_header_bytes(size_t *total, size_t name_len, size_t value_len,
                             size_t limit)
{
    if (name_len > limit || value_len > limit - name_len ||
        limit - name_len - value_len < 32 ||
        *total > limit - name_len - value_len - 32) {
        return false;
    }
    *total += name_len + value_len + 32;
    return true;
}

static bool equals(zerobus_string_view_t value, const char *literal)
{
    return value.len == strlen(literal) &&
           memcmp(value.data, literal, value.len) == 0;
}

/* ---- call completion --------------------------------------------------- */

static void release_send(zb_grpc_t *grpc)
{
    zb_message_t *message = grpc->sending;
    if (message == NULL) {
        return;
    }
    grpc->sending = NULL;
    free(message->data);
    free(message);
    grpc->send_prefix_used = 0;
    grpc->send_payload_used = 0;
}

/* Finishes the call exactly once: frees owned messages, then passes the result
 * and ownership of grpc_message to on_complete. No-op before a request exists
 * or after completion. */
static void complete(zb_grpc_t *grpc, zerobus_status_t status,
                     bool has_grpc_status, uint32_t http2_error)
{
    if (grpc->id == 0 || grpc->completed) {
        return;
    }
    grpc->completed = true;
    release_send(grpc);
    decoder_clear(&grpc->decoder);
    const zb_rpc_result_t result = {.status = status,
                                    .has_grpc_status = has_grpc_status,
                                    .grpc_status = grpc->grpc_status,
                                    .http_status = grpc->http_status,
                                    .http2_error = http2_error,
                                    .message = grpc->grpc_message};
    grpc->grpc_message = NULL;
    if (grpc->config.on_complete != NULL) {
        grpc->config.on_complete(grpc->config.context, &result);
    } else {
        free(result.message);
    }
}

/* Queues RST_STREAM with error, which also tells the peer to stop sending. */
static int reset_stream(zb_grpc_t *grpc, uint32_t error)
{
    int result = nghttp2_submit_rst_stream(grpc->session, NGHTTP2_FLAG_NONE,
                                           grpc->id, error);
    return result == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
}

static int close_call(zb_grpc_t *grpc, zerobus_status_t status,
                      bool has_grpc_status, uint32_t error)
{
    complete(grpc, status, has_grpc_status, error);
    return reset_stream(grpc, error);
}

/* Completes the call for an invalid response header. The returned
 * TEMPORAL_CALLBACK_FAILURE makes nghttp2 close the stream itself. */
static int bad_header(zb_grpc_t *grpc, zerobus_status_t status)
{
    complete(grpc, status, false, NGHTTP2_PROTOCOL_ERROR);
    return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
}

static int decode(zb_grpc_t *grpc, const uint8_t *data, size_t len,
                  size_t *consumed)
{
    if (grpc->completed) {
        grpc->paused = false;
        return 0;
    }
    zerobus_status_t result = decoder_feed(
        &grpc->decoder, data, len, grpc->config.max_message_bytes,
        grpc->config.on_message, grpc->config.context, consumed, &grpc->paused);
    if (result == ZEROBUS_STATUS_OK) {
        return 0;
    }
    return close_call(grpc, result, false, NGHTTP2_CANCEL);
}

/* ---- nghttp2 callbacks: outgoing --------------------------------------- */

/* Called from nghttp2_session_mem_send whenever the request may send DATA, and
 * again after nghttp2_session_resume_data. Fills buffer with up to length bytes
 * of framed messages from pull_send. */
static ssize_t on_data_source_read(nghttp2_session *session, int32_t stream_id,
                                   uint8_t *buffer, size_t length,
                                   uint32_t *data_flags,
                                   nghttp2_data_source *source, void *user_data)
{
    (void)session;
    (void)stream_id;
    (void)source;
    zb_grpc_t *grpc = user_data;
    size_t written = 0;
    if (grpc->completed) {
        return NGHTTP2_ERR_DEFERRED;
    }
    for (;;) {
        if (grpc->sending == NULL) {
            bool eof = false;
            grpc->sending = grpc->config.pull_send(grpc->config.context, &eof);
            if (grpc->sending == NULL) {
                if (eof) {
                    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
                    grpc->request_closed = true;
                }
                break;
            }
            if (grpc->sending->len > UINT32_MAX) {
                complete(grpc, ZEROBUS_STATUS_RESOURCE_EXHAUSTED, false,
                         NGHTTP2_INTERNAL_ERROR);
                return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
            }
            write_prefix(grpc->send_prefix, (uint32_t)grpc->sending->len);
        }
        if (written == length) {
            break;
        }
        zb_message_t *message = grpc->sending;
        fill_from_input(buffer, length, &written, grpc->send_prefix, 5,
                        &grpc->send_prefix_used);
        fill_from_input(buffer, length, &written, message->data, message->len,
                        &grpc->send_payload_used);
        if (grpc->send_prefix_used == 5 &&
            grpc->send_payload_used == message->len) {
            release_send(grpc);
        }
    }
    if (written == 0 && !grpc->request_closed) {
        return NGHTTP2_ERR_DEFERRED;
    }
    return (ssize_t)written;
}

/* Called from nghttp2_session_mem_send after each frame is sent. For a GOAWAY
 * with an error code, aborts the call and returns a session-fatal error */
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
 * was the request HEADERS, completes the call as UNAVAILABLE. */
static int on_frame_not_send(nghttp2_session *session,
                             const nghttp2_frame *frame, int error_code,
                             void *user_data)
{
    (void)session;
    (void)error_code;
    zb_grpc_t *grpc = user_data;
    if (frame->hd.type == NGHTTP2_HEADERS && frame->hd.stream_id == grpc->id) {
        complete(grpc, ZEROBUS_STATUS_UNAVAILABLE, false, 0);
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
    zb_grpc_t *grpc = user_data;
    if (grpc->completed || frame->hd.stream_id != grpc->id) {
        return 0;
    }
    if (!add_header_bytes(&grpc->header_bytes, name_len, value_len,
                          grpc->config.max_header_bytes)) {
        return bad_header(grpc, ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    }
    zerobus_string_view_t key = {(const char *)name, name_len};
    if (equals(key, ":status")) {
        if (grpc->block_http_status != 0 || value_len != 3 ||
            !parse_decimal(value, value_len, &grpc->block_http_status) ||
            grpc->block_http_status < 100) {
            return bad_header(grpc, ZEROBUS_STATUS_INTERNAL);
        }
    } else if (equals(key, "content-type")) {
        if (grpc->content_type_seen) {
            return bad_header(grpc, ZEROBUS_STATUS_INTERNAL);
        }
        grpc->content_type_seen = true;
        grpc->content_type_valid = is_grpc_content_type(value, value_len);
    } else if (equals(key, "grpc-status")) {
        if (grpc->grpc_status_seen ||
            !parse_decimal(value, value_len, &grpc->grpc_status)) {
            return bad_header(grpc, ZEROBUS_STATUS_INTERNAL);
        }
        grpc->grpc_status_seen = true;
    } else if (equals(key, "grpc-message")) {
        if (grpc->grpc_message != NULL) {
            return bad_header(grpc, ZEROBUS_STATUS_INTERNAL);
        }
        grpc->grpc_message = malloc(value_len + 1);
        if (grpc->grpc_message == NULL) {
            return bad_header(grpc, ZEROBUS_STATUS_OUT_OF_MEMORY);
        }
        memcpy(grpc->grpc_message, value, value_len);
        percent_decode(grpc->grpc_message, value_len);
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
        if (!grpc->completed &&
            (grpc->id > frame->goaway.last_stream_id ||
             frame->goaway.error_code != NGHTTP2_NO_ERROR)) {
            complete(grpc, ZEROBUS_STATUS_UNAVAILABLE, false,
                     frame->goaway.error_code);
            return reset_stream(grpc, NGHTTP2_CANCEL);
        }
        return 0;
    }
    if (grpc->completed || frame->hd.stream_id != grpc->id) {
        return 0;
    }
    bool end = (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0;
    if (frame->hd.type == NGHTTP2_HEADERS) {
        if (grpc->http_status == 0) {
            if (grpc->block_http_status == 0) {
                goto invalid;
            }
            if (grpc->block_http_status < 200) {
                if (end || grpc->grpc_status_seen ||
                    grpc->grpc_message != NULL) {
                    goto invalid;
                }
                goto next_headers;
            }
            grpc->http_status = grpc->block_http_status;
            if (!grpc->content_type_valid) {
                if (grpc->http_status != 200 && !grpc->grpc_status_seen) {
                    return close_call(grpc, map_http_status(grpc->http_status),
                                      false, NGHTTP2_CANCEL);
                }
                goto invalid;
            }
        } else if (grpc->block_http_status != 0 || !end ||
                   (grpc->content_type_seen && !grpc->content_type_valid)) {
            goto invalid;
        }
        if (!end) {
            if (grpc->grpc_status_seen || grpc->grpc_message != NULL) {
                goto invalid;
            }
            goto next_headers;
        }
    } else if (frame->hd.type != NGHTTP2_DATA || !end) {
        return 0;
    }
    if (!decoder_between_messages(&grpc->decoder)) {
        goto invalid;
    }
    bool has_status =
        frame->hd.type == NGHTTP2_HEADERS && grpc->grpc_status_seen;
    zerobus_status_t status = has_status ? map_grpc_status(grpc->grpc_status)
                                         : map_http_status(grpc->http_status);
    if (!grpc->request_closed) {
        return close_call(grpc, status, has_status, NGHTTP2_NO_ERROR);
    }
    complete(grpc, status, has_status, NGHTTP2_NO_ERROR);
    return 0;

next_headers:
    grpc->header_bytes = 0;
    grpc->block_http_status = 0;
    grpc->content_type_seen = false;
    grpc->content_type_valid = false;
    return 0;

invalid:
    return close_call(grpc, ZEROBUS_STATUS_INTERNAL, false,
                      NGHTTP2_PROTOCOL_ERROR);
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
    if (grpc->completed || stream_id != grpc->id) {
        return 0;
    }
    if (grpc->http_status == 0) {
        return close_call(grpc, ZEROBUS_STATUS_INTERNAL, false,
                          NGHTTP2_PROTOCOL_ERROR);
    }
    size_t consumed = 0;
    int result = decode(grpc, data, len, &consumed);
    if (result != 0 || !grpc->paused) {
        return result;
    }
    grpc->remainder = data + consumed;
    grpc->remainder_len = len - consumed;
    return NGHTTP2_ERR_PAUSE;
}

/* Called when the stream closes for any reason. */
static int on_stream_close(nghttp2_session *session, int32_t stream_id,
                           uint32_t error_code, void *user_data)
{
    (void)session;
    zb_grpc_t *grpc = user_data;
    if (grpc->id == stream_id) {
        complete(grpc, map_http2_error(error_code), false, error_code);
    }
    return 0;
}

/* ---- request submission ------------------------------------------------ */

/* Checks a :path (must start with '/') or :authority (no '/', '?' or '@')
 * value: non-empty visible ASCII without '#'. */
static bool valid_target(zerobus_string_view_t value, bool authority)
{
    if (value.data == NULL || value.len == 0 ||
        (!authority && value.data[0] != '/')) {
        return false;
    }
    for (size_t i = 0; i < value.len; ++i) {
        unsigned char c = (unsigned char)value.data[i];
        if (c < 0x21 || c > 0x7e || c == '#' ||
            (authority && (c == '/' || c == '?' || c == '@'))) {
            return false;
        }
    }
    return true;
}

/* Checks one caller metadata entry: a lowercase gRPC header name, other than
 * the grpc-* names and headers that the transport owns or HTTP/2 forbids, with
 * a printable-ASCII value. */
static bool valid_metadata(const zb_metadata_t *header)
{
    if (header->name.data == NULL || header->name.len == 0 ||
        (header->value.data == NULL && header->value.len != 0)) {
        return false;
    }
    for (size_t i = 0; i < header->name.len; ++i) {
        char c = header->name.data[i];
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' ||
              c == '-' || c == '.')) {
            return false;
        }
    }
    static const char *const reserved[] = {"content-type",
                                           "content-length",
                                           "te",
                                           "host",
                                           "connection",
                                           "keep-alive",
                                           "proxy-connection",
                                           "transfer-encoding",
                                           "upgrade"};
    if (header->name.len >= 5 && memcmp(header->name.data, "grpc-", 5) == 0) {
        return false;
    }
    for (size_t i = 0; i < sizeof(reserved) / sizeof(reserved[0]); ++i) {
        if (equals(header->name, reserved[i])) {
            return false;
        }
    }
    for (size_t i = 0; i < header->value.len; ++i) {
        unsigned char c = (unsigned char)header->value.data[i];
        if (c < 0x20 || c > 0x7e) {
            return false;
        }
    }
    return true;
}

static nghttp2_nv make_nv(const zb_metadata_t *header)
{
    nghttp2_nv nv = {(uint8_t *)header->name.data,
                     (uint8_t *)header->value.data, header->name.len,
                     header->value.len, NGHTTP2_NV_FLAG_NONE};
    if (equals(header->name, "authorization")) {
        nv.flags = NGHTTP2_NV_FLAG_NO_INDEX;
    }
    return nv;
}

/* Queues a call on an initialized session. Nothing is sent: the call opens on
 * the wire when the owner next calls nghttp2_session_mem_send. */
static zerobus_status_t queue_call(zb_grpc_t *grpc,
                                   const zb_grpc_config_t *config,
                                   zerobus_string_view_t path,
                                   const zb_metadata_t *headers,
                                   size_t header_count)
{
    if (header_count > config->max_header_bytes / 32 ||
        header_count > SIZE_MAX / sizeof(nghttp2_nv) - 6 ||
        path.len > config->max_header_bytes) {
        return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    }
    if (!valid_target(path, false)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    const zb_metadata_t protocol[] = {
        {zerobus_string_view(":method"), zerobus_string_view("POST")},
        {zerobus_string_view(":scheme"),
         zerobus_string_view(config->tls ? "https" : "http")},
        {zerobus_string_view(":authority"), config->authority},
        {zerobus_string_view(":path"), path},
        {zerobus_string_view("content-type"),
         zerobus_string_view("application/grpc")},
        {zerobus_string_view("te"), zerobus_string_view("trailers")}};
    const size_t protocol_count = sizeof(protocol) / sizeof(protocol[0]);
    const size_t count = protocol_count + header_count;
    size_t total = 0;
    for (size_t i = 0; i < count; ++i) {
        const zb_metadata_t *header =
            i < protocol_count ? &protocol[i] : &headers[i - protocol_count];
        if (!add_header_bytes(&total, header->name.len, header->value.len,
                              config->max_header_bytes)) {
            return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
        }
        if (i >= protocol_count && !valid_metadata(header)) {
            return ZEROBUS_STATUS_INVALID_ARGUMENT;
        }
    }
    nghttp2_nv *nva = malloc(count * sizeof(*nva));
    if (nva == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    for (size_t i = 0; i < count; ++i) {
        const zb_metadata_t *header =
            i < protocol_count ? &protocol[i] : &headers[i - protocol_count];
        nva[i] = make_nv(header);
    }
    const nghttp2_data_provider provider = {{0}, on_data_source_read};
    int32_t id = nghttp2_submit_request(grpc->session, NULL, nva, count,
                                        &provider, NULL);
    free(nva);
    if (id < 0) {
        return id == NGHTTP2_ERR_NOMEM ? ZEROBUS_STATUS_OUT_OF_MEMORY
                                       : ZEROBUS_STATUS_UNAVAILABLE;
    }
    grpc->id = id;
    return ZEROBUS_STATUS_OK;
}

/* ---- component operations ---------------------------------------------- */

zerobus_status_t zb_grpc_init(zb_grpc_t *grpc, const zb_grpc_config_t *config,
                              zerobus_string_view_t path,
                              const zb_metadata_t *headers, size_t header_count)
{
    if (config == NULL || grpc == NULL ||
        (headers == NULL && header_count != 0) ||
        config->max_header_bytes == 0 ||
        config->max_header_bytes > UINT32_MAX ||
        config->max_message_bytes == 0 ||
        config->max_message_bytes > UINT32_MAX ||
        config->authority.len > config->max_header_bytes ||
        config->on_message == NULL || config->pull_send == NULL ||
        !valid_target(config->authority, true)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zerobus_status_t status = ZEROBUS_STATUS_OUT_OF_MEMORY;
    nghttp2_session_callbacks *callbacks = NULL;
    nghttp2_option *option = NULL;
    grpc->config = *config;
    /* Authority is not used after initialization. Retain no caller strings. */
    grpc->config.authority = (zerobus_string_view_t){0};
    if (nghttp2_session_callbacks_new(&callbacks) != 0 ||
        nghttp2_option_new(&option) != 0) {
        goto cleanup;
    }
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
    if (nghttp2_session_client_new2(&grpc->session, callbacks, grpc, option) !=
        0) {
        goto cleanup;
    }
    const nghttp2_settings_entry settings[] = {
        {NGHTTP2_SETTINGS_ENABLE_PUSH, 0},
        {NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE,
         (uint32_t)config->max_header_bytes}};
    if (nghttp2_submit_settings(grpc->session, NGHTTP2_FLAG_NONE, settings,
                                sizeof(settings) / sizeof(settings[0])) != 0) {
        goto cleanup;
    }
    status = queue_call(grpc, config, path, headers, header_count);
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
        release_send(grpc);
        decoder_clear(&grpc->decoder);
        free(grpc->grpc_message);
        *grpc = (zb_grpc_t){0};
    }
}

void zb_grpc_abort(zb_grpc_t *grpc, zerobus_status_t status)
{
    if (grpc != NULL && grpc->failure == ZEROBUS_STATUS_OK) {
        grpc->failure =
            status == ZEROBUS_STATUS_OK ? ZEROBUS_STATUS_UNAVAILABLE : status;
        complete(grpc, grpc->failure, false, NGHTTP2_NO_ERROR);
    }
}

zerobus_status_t zb_grpc_retry_receive(zb_grpc_t *grpc)
{
    if (!grpc->paused) {
        return grpc->failure;
    }
    size_t consumed = 0;
    int result = decode(grpc, grpc->remainder, grpc->remainder_len, &consumed);
    if (consumed != 0) {
        grpc->remainder += consumed;
        grpc->remainder_len -= consumed;
    }
    if (result < 0) {
        zb_grpc_abort(grpc, result == NGHTTP2_ERR_NOMEM
                                ? ZEROBUS_STATUS_OUT_OF_MEMORY
                                : ZEROBUS_STATUS_INTERNAL);
    }
    return grpc->failure;
}
