#ifndef ZB_RECORD_H
#define ZB_RECORD_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "zerobus/common.h"

/* Placeholder until the protobuf request representation is settled. */
typedef struct zb_ingest_request {
    size_t len;
    char data[];
} zb_ingest_request_t;

/* Owns a NUL-terminated copy of data.
 * Returns NULL on size overflow, allocation failure, or NULL data with nonzero
 * length. */
static inline zb_ingest_request_t *
zb_ingest_request_new(zerobus_string_view_t data)
{
    if ((data.data == NULL && data.len != 0) ||
        data.len > SIZE_MAX - sizeof(zb_ingest_request_t) - 1) {
        return NULL;
    }
    zb_ingest_request_t *request =
        (zb_ingest_request_t *)malloc(sizeof(*request) + data.len + 1);
    if (request == NULL) {
        return NULL;
    }
    request->len = data.len;
    if (data.len != 0) {
        memcpy(request->data, data.data, data.len);
    }
    request->data[data.len] = '\0';
    return request;
}

/* Frees the request and its payload. Accepts NULL. */
static inline void zb_ingest_request_free(zb_ingest_request_t *request)
{
    free(request);
}

#endif /* ZB_RECORD_H */
