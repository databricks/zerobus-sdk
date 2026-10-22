#ifndef ZB_NETWORK_MESSAGE_H
#define ZB_NETWORK_MESSAGE_H

#include "zerobus/common.h"
#include <stddef.h>
#include <stdint.h>

typedef struct {
    zerobus_string_view_t name;
    zerobus_string_view_t value;
} zb_metadata_t;

/* Descriptor and data are individually malloc-owned. NULL data is permitted
 * for empty messages.
 *
 * So each message costs two allocations. Getting it down to one depends on how
 * the transport's channels hold messages:
 * - Channels of zb_message_t values (a new queue, or common/queue generalized
 *   to a fixed element size): pass zb_message_t by value and transfer only
 *   data. The encoder's (data, len) output then goes in unchanged. The cost
 *   is that the channels can't reuse common/queue as it is.
 * - Channels of pointers (common/queue as it is): keep zb_message_t *, but put
 *   the header and the payload in one block (size_t len; uint8_t data[]),
 *   with the encoder writing the record into it. The cost is that the encoder
 *   must then know this type and its layout, instead of returning plain
 *   (data, len). */
typedef struct {
    uint8_t *data;
    size_t len;
} zb_message_t;

typedef struct {
    /* Mapped gRPC status if has_grpc_status, otherwise local failure. */
    zerobus_status_t status;
    bool has_grpc_status;
    uint32_t grpc_status;
    uint32_t http_status;
    /* RST_STREAM code sent or received, or the peer's GOAWAY code; 0 if
     * none. */
    uint32_t http2_error;
    char *message;
} zb_rpc_result_t;

#endif /* ZB_NETWORK_MESSAGE_H */
