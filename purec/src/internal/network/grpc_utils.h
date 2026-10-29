/* Helpers for the gRPC transport (grpc.c) that need no session or call
 * state: message framing, status mapping, and header checks. */
#ifndef ZB_NETWORK_GRPC_UTILS_H
#define ZB_NETWORK_GRPC_UTILS_H

#include <nghttp2/nghttp2.h>

#include "message.h"
#include "zerobus/common.h"

/* An incoming message being assembled across DATA chunks. */
typedef struct {
    uint8_t prefix[5];
    size_t prefix_used;
    size_t payload_used;
    zb_message_t *message;
} zb_grpc_decoder_t;

void zb_write_grpc_prefix(uint8_t prefix[5], uint32_t length);

void zb_grpc_decoder_clear(zb_grpc_decoder_t *decoder);

bool zb_grpc_decoder_between_messages(const zb_grpc_decoder_t *decoder);

/* Decodes gRPC messages from a stream that arrives in chunks, one chunk per
 * call. A message split across chunks is completed by later calls.
 *
 * deliver is the output: it receives each complete message in order and
 * returns true to take ownership of it. It returns false to refuse the message
 * for now (e.g. no room downstream): the decoder keeps it, stops right after
 * it, and sets *paused. While paused, the next call, made with the rest of the
 * same input, offers the kept message again. */
zerobus_status_t zb_grpc_decoder_feed(zb_grpc_decoder_t *decoder,
                                      const uint8_t *data, size_t len,
                                      size_t maximum,
                                      bool (*deliver)(void *, zb_message_t *),
                                      void *context, size_t *consumed,
                                      bool *paused);

zerobus_status_t zb_map_grpc_status(uint32_t code);

zerobus_status_t zb_map_http_status(uint32_t code);

zerobus_status_t zb_map_http2_error(uint32_t error);

zerobus_status_t zb_map_nghttp2_error(int error);

/* True for application/grpc, matched case-insensitively and optionally followed
 * by a "+subtype" or ";parameters". */
bool zb_is_grpc_content_type(const uint8_t *value, size_t len);

/* Percent-decodes a grpc-message value in place and NUL-terminates it, so value
 * needs len + 1 bytes. Malformed escapes remain literal as required by gRPC. */
void zb_grpc_percent_decode(char *value, size_t len);

/* Adds one header field's size to *total, counted the way HTTP/2 sizes header
 * lists (RFC 7540, 6.5.2): name + value + 32 octets of per-field overhead.
 * Returns false, leaving *total unchanged, when that would exceed limit. */
bool zb_add_http2_header_bytes(size_t *total, size_t name_len, size_t value_len,
                               size_t limit);

/* Checks a :path (must start with '/') or :authority (no '/', '?' or '@')
 * value: non-empty visible ASCII without '#'. */
bool zb_is_valid_grpc_target(zerobus_string_view_t value, bool authority);

/* Checks one caller metadata entry: a lowercase gRPC header name, other than
 * the grpc-* names and headers that the transport owns or HTTP/2 forbids, with
 * a printable-ASCII value. */
bool zb_is_valid_grpc_metadata(const zb_metadata_t *header);

nghttp2_nv zb_make_nghttp2_nv(const zb_metadata_t *header);

#endif /* ZB_NETWORK_GRPC_UTILS_H */
