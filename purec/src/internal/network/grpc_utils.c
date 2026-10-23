#include <stdlib.h>
#include <string.h>

#include "grpc_utils.h"
#include "internal/common/utils.h"

/* ---- message framing --------------------------------------------------- */

void zb_write_grpc_prefix(uint8_t prefix[5], uint32_t length)
{
    prefix[0] = 0; /* uncompressed flag */
    prefix[1] = (uint8_t)(length >> 24);
    prefix[2] = (uint8_t)(length >> 16);
    prefix[3] = (uint8_t)(length >> 8);
    prefix[4] = (uint8_t)length;
}

void zb_grpc_decoder_clear(zb_grpc_decoder_t *decoder)
{
    if (decoder->message != NULL) {
        free(decoder->message->data);
        free(decoder->message);
    }
    memset(decoder, 0, sizeof(*decoder));
}

bool zb_grpc_decoder_between_messages(const zb_grpc_decoder_t *decoder)
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
        return ZEROBUS_STATUS_OUT_OF_RANGE;
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

zerobus_status_t zb_grpc_decoder_feed(zb_grpc_decoder_t *decoder,
                                      const uint8_t *data, size_t len,
                                      size_t maximum,
                                      bool (*deliver)(void *, zb_message_t *),
                                      void *context, size_t *consumed,
                                      bool *paused)
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
            zb_copy_bounded(decoder->prefix, 5, &decoder->prefix_used, data,
                            len, consumed);
            if (decoder->prefix_used == 5) {
                status = start_message(decoder, maximum);
            }
        } else {
            zb_copy_bounded(message->data, message->len, &decoder->payload_used,
                            data, len, consumed);
        }
    }
    return status;
}

/* ---- status mapping ---------------------------------------------------- */

zerobus_status_t zb_map_grpc_status(uint32_t code)
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
    case 11:
        return ZEROBUS_STATUS_OUT_OF_RANGE;
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

zerobus_status_t zb_map_http_status(uint32_t code)
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

zerobus_status_t zb_map_http2_error(uint32_t error)
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

zerobus_status_t zb_map_nghttp2_error(int error)
{
    switch (error) {
    case NGHTTP2_ERR_NOMEM:
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    case NGHTTP2_ERR_STREAM_ID_NOT_AVAILABLE:
        return ZEROBUS_STATUS_UNAVAILABLE;
    default:
        return ZEROBUS_STATUS_INTERNAL;
    }
}

/* ---- header values ----------------------------------------------------- */

bool zb_is_grpc_content_type(const uint8_t *value, size_t len)
{
    static const char type[] = "application/grpc";
    const size_t prefix = sizeof(type) - 1;
    return zb_starts_with((const char *)value, len, type) &&
           (len == prefix || value[prefix] == '+' || value[prefix] == ';');
}

void zb_grpc_percent_decode(char *value, size_t len)
{
    size_t out = 0;
    for (size_t i = 0; i < len; ++i) {
        char c = value[i];
        if (c == '%' && len - i >= 3) {
            int high = zb_hex_value(value[i + 1]);
            int low = zb_hex_value(value[i + 2]);
            if (high >= 0 && low >= 0) {
                c = (char)(high * 16 + low);
                i += 2;
            }
        }
        value[out++] = c == '\0' ? '?' : c;
    }
    value[out] = '\0';
}

bool zb_add_http2_header_bytes(size_t *total, size_t name_len, size_t value_len,
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

/* ---- request headers --------------------------------------------------- */

bool zb_is_valid_grpc_target(zerobus_string_view_t value, bool authority)
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

bool zb_is_valid_grpc_metadata(const zb_metadata_t *header)
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
        if (zb_view_equals(header->name, reserved[i])) {
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

nghttp2_nv zb_make_nghttp2_nv(const zb_metadata_t *header)
{
    nghttp2_nv nv = {(uint8_t *)header->name.data,
                     (uint8_t *)header->value.data, header->name.len,
                     header->value.len, NGHTTP2_NV_FLAG_NONE};
    if (zb_view_equals(header->name, "authorization")) {
        nv.flags = NGHTTP2_NV_FLAG_NO_INDEX;
    }
    return nv;
}
