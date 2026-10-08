#ifndef ZB_RECORD_H
#define ZB_RECORD_H

#include <stddef.h>
#include <stdlib.h>

#include "utils.h"
#include "zerobus/common.h"

typedef struct zb_record {
    char *json;
    size_t len;
} zb_record_t;

/* Owns a copy of json. Non-empty payloads are NUL-terminated.
 * Returns NULL on allocation failure or NULL data with nonzero length. */
static inline zb_record_t *zb_record_new(zerobus_string_view_t json)
{
    if (json.data == NULL && json.len != 0) {
        return NULL;
    }
    zb_record_t *record = (zb_record_t *)calloc(1, sizeof(*record));
    if (record == NULL) {
        return NULL;
    }
    if (json.len == 0) {
        return record;
    }
    record->json = zb_strdup_view(json);
    record->len = json.len;
    if (record->json == NULL) {
        free(record);
        return NULL;
    }
    return record;
}

/* Frees the record and its payload. Accepts NULL. */
static inline void zb_record_free(zb_record_t *record)
{
    if (record != NULL) {
        free(record->json);
        free(record);
    }
}

#endif /* ZB_RECORD_H */
