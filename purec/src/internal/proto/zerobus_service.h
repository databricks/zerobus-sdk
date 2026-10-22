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
 * Input must be valid UTF-8 without raw NUL bytes; empty input is permitted.
 * On success *out_data is a malloc'd block of *out_len bytes that the caller
 * owns and releases with free().
 */
zerobus_status_t zb_proto_encode_create_stream(zerobus_string_view_t table_name,
                                               uint8_t **out_data,
                                               size_t *out_len);

/* Encode EphemeralStreamRequest{ingest_record: {offset_id, json_record}}.
 * Input and ownership as for zb_proto_encode_create_stream. */
zerobus_status_t
zb_proto_encode_ingest_record(zerobus_offset_t offset_id,
                              zerobus_string_view_t json_record,
                              uint8_t **out_data, size_t *out_len);

/* Size of the JsonRecordBatch message holding records, or 0 if it would not
 * fit an encoded payload. */
size_t zb_proto_json_batch_size(const zerobus_string_view_t *records,
                                size_t count);

/* Writes the JsonRecordBatch message holding records into out, which has
 * zb_proto_json_batch_size bytes. */
void zb_proto_write_json_batch(const zerobus_string_view_t *records,
                               size_t count, uint8_t *out);

/* Encode EphemeralStreamRequest{ingest_record_batch: {offset_id, json_batch}}
 * from json_batch, a message written by zb_proto_write_json_batch. Ownership
 * as for zb_proto_encode_create_stream. */
zerobus_status_t zb_proto_encode_ingest_record_batch(zerobus_offset_t offset_id,
                                                     const uint8_t *json_batch,
                                                     size_t json_batch_len,
                                                     uint8_t **out_data,
                                                     size_t *out_len);

/* Most bytes zb_proto_write_ingest_header writes. */
#define ZB_PROTO_INGEST_HEADER_MAX 33u

/* Writes into out the bytes that precede the data in
 * EphemeralStreamRequest{ingest_record} for a JSON record of len bytes, or
 * with batch in EphemeralStreamRequest{ingest_record_batch} for a
 * JsonRecordBatch message of len bytes, at offset_id. Returns their count,
 * at most ZB_PROTO_INGEST_HEADER_MAX: the len data bytes complete the
 * request. */
size_t zb_proto_write_ingest_header(uint8_t *out, zerobus_offset_t offset_id,
                                    size_t len, bool batch);

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
 * and OUT_OF_MEMORY when an allocation fails.
 * Treats *out_response as uninitialized and overwrites it without releasing
 * owned fields. Clear a previous result with zb_response_clear before decoding
 * into the same output again.
 * On failure *out_response is left empty. Release with zb_response_clear.
 */
zerobus_status_t zb_proto_decode_response(const uint8_t *data, size_t len,
                                          zb_response_t *out_response);

/* Decodes data when it is exactly an EphemeralStreamResponse holding an
 * ingest_record_response with an offset, the common case, without
 * allocating. False for anything else, which zb_proto_decode_response then
 * handles. */
bool zb_proto_decode_ack(const uint8_t *data, size_t len,
                         zerobus_offset_t *out_offset);

/* Release owned fields and reset to NONE. NULL-safe. */
void zb_response_clear(zb_response_t *response);

#endif /* ZB_PROTO_ZEROBUS_SERVICE_H */
