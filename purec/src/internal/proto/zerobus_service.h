/*
 * Codec for the EphemeralStream envelope messages of
 * rust/sdk/zerobus_service.proto (package databricks.zerobus).
 */
#ifndef ZB_PROTO_ZEROBUS_SERVICE_H
#define ZB_PROTO_ZEROBUS_SERVICE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "zerobus/common.h"

/*
 * See "Known protobuf-c compatibility behavior" in
 * tests/src/internal/proto/zerobus_service_test.c for accepted encoding and
 * decoding edge cases.
 */

/*
 * Encode EphemeralStreamRequest{create_stream: {table_name, record_type:JSON}}.
 * On success *out_data is a malloc'd block of *out_len bytes that the caller
 * owns and releases with free().
 */
zerobus_status_t zb_proto_encode_create_stream(zerobus_string_view_t table_name,
                                               uint8_t **out_data,
                                               size_t *out_len);

/* Encode EphemeralStreamRequest{ingest_record: {offset_id, json_record}}.
 * Ownership as for zb_proto_encode_create_stream. */
zerobus_status_t
zb_proto_encode_ingest_record(zerobus_offset_t offset_id,
                              zerobus_string_view_t json_record,
                              uint8_t **out_data, size_t *out_len);

typedef enum zb_response_kind {
    ZB_RESPONSE_NONE = 0, /* no payload, or one this SDK does not know */
    ZB_RESPONSE_CREATE_STREAM,
    ZB_RESPONSE_INGEST_RECORD,
    ZB_RESPONSE_CLOSE_STREAM_SIGNAL
} zb_response_kind_t;

/* A decoded EphemeralStreamResponse. has_* flags report optional fields. */
typedef struct zb_response {
    zb_response_kind_t kind;
    union {
        struct {
            char *stream_id; /* owned, NUL-terminated; NULL when absent */
        } create_stream;
        struct {
            bool has_durability_ack_up_to_offset;
            zerobus_offset_t durability_ack_up_to_offset;
        } ingest_record;
        struct {
            bool has_duration;
            int64_t duration_seconds;
            int32_t duration_nanos;
        } close_stream_signal;
    } payload;
} zb_response_t;

/*
 * Decode an EphemeralStreamResponse. Unknown fields with supported wire types
 * are skipped. Returns INVALID_ARGUMENT for malformed or unsupported input
 * and OUT_OF_MEMORY when a copy fails.
 * On failure *out_response is left empty. Release with zb_response_clear.
 */
zerobus_status_t zb_proto_decode_response(const uint8_t *data, size_t len,
                                          zb_response_t *out_response);

/* Release owned fields and reset to NONE. NULL-safe. */
void zb_response_clear(zb_response_t *response);

#endif /* ZB_PROTO_ZEROBUS_SERVICE_H */
