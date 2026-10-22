#ifndef ZB_INGEST_REQUEST_H
#define ZB_INGEST_REQUEST_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "zerobus/common.h"

/* One ingest request: a single JSON record (records == 1), or with
 * records > 1 the encoded JsonRecordBatch body holding them. */
typedef struct zb_ingest_request {
    size_t len;
    size_t records;
    size_t capacity; /* Bytes data can hold before its NUL, at least len. */
    char data[];
} zb_ingest_request_t;

/* Allocates a request holding up to capacity bytes, at least len, with len
 * bytes of data left uninitialized except for the NUL after them. Returns
 * NULL on size overflow or allocation failure. */
static inline zb_ingest_request_t *
zb_ingest_request_reserve(size_t len, size_t records, size_t capacity)
{
    if (capacity < len) {
        capacity = len;
    }
    if (capacity > SIZE_MAX - sizeof(zb_ingest_request_t) - 1) {
        return NULL;
    }
    zb_ingest_request_t *request =
        (zb_ingest_request_t *)malloc(sizeof(*request) + capacity + 1);
    if (request == NULL) {
        return NULL;
    }
    request->len = len;
    request->records = records;
    request->capacity = capacity;
    request->data[len] = '\0';
    return request;
}

/* zb_ingest_request_reserve with no spare capacity. */
static inline zb_ingest_request_t *zb_ingest_request_alloc(size_t len,
                                                           size_t records)
{
    return zb_ingest_request_reserve(len, records, len);
}

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
    zb_ingest_request_t *request = zb_ingest_request_alloc(data.len, 1);
    if (request != NULL && data.len != 0) {
        memcpy(request->data, data.data, data.len);
    }
    return request;
}

/* Frees the request and its payload. Accepts NULL. */
static inline void zb_ingest_request_free(zb_ingest_request_t *request)
{
    free(request);
}

#endif /* ZB_INGEST_REQUEST_H */
