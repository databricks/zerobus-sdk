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
 * for empty messages. A framed message's data already holds whole gRPC
 * messages, each with its prefix, and is sent as is. */
typedef struct {
    uint8_t *data;
    size_t len;
    bool framed;
} zb_message_t;

typedef struct {
    /* Mapped gRPC status if has_grpc_status, otherwise local failure. */
    zerobus_status_t status;
    bool has_grpc_status;
    uint32_t grpc_status;
    uint32_t http_status;
    uint32_t http2_error;
    char *message;
} zb_rpc_result_t;

#endif /* ZB_NETWORK_MESSAGE_H */
