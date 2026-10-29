#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "internal/common/utils.h"
#include "internal/proto/zerobus_service.h"
#include "zerobus_service.pb-c.h"

static bool check_args(zerobus_string_view_t input, uint8_t **out_data,
                       size_t *out_len)
{
    if (out_data == NULL || out_len == NULL ||
        (input.data == NULL && input.len > 0) ||
        input.len > ZB_MAX_ENCODED_PAYLOAD) {
        return false;
    }
    return true;
}

static zerobus_status_t
pack_request(const Databricks__Zerobus__EphemeralStreamRequest *request,
             uint8_t **out_data, size_t *out_len)
{
    size_t len =
        databricks__zerobus__ephemeral_stream_request__get_packed_size(request);
    uint8_t *data = (uint8_t *)malloc(len);
    if (data == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    size_t written =
        databricks__zerobus__ephemeral_stream_request__pack(request, data);
    if (written != len) {
        free(data);
        return ZEROBUS_STATUS_INTERNAL;
    }
    *out_data = data;
    *out_len = len;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_proto_encode_create_stream(zerobus_string_view_t table_name,
                                               uint8_t **out_data,
                                               size_t *out_len)
{
    if (!check_args(table_name, out_data, out_len)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    char *table =
        zb_is_empty(table_name) ? zb_strdup("") : zb_strdup_view(table_name);
    if (table == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }

    Databricks__Zerobus__CreateIngestStreamRequest create;
    databricks__zerobus__create_ingest_stream_request__init(&create);
    create.table_name = table;
    create.has_record_type = 1;
    create.record_type = DATABRICKS__ZEROBUS__RECORD_TYPE__JSON;

    Databricks__Zerobus__EphemeralStreamRequest request;
    databricks__zerobus__ephemeral_stream_request__init(&request);
    request.payload_case =
        DATABRICKS__ZEROBUS__EPHEMERAL_STREAM_REQUEST__PAYLOAD_CREATE_STREAM;
    request.create_stream = &create;

    zerobus_status_t status = pack_request(&request, out_data, out_len);
    free(table);
    return status;
}

static size_t varint_size(uint64_t value)
{
    size_t len = 1;
    while (value >= 0x80) {
        value >>= 7;
        len++;
    }
    return len;
}

static uint8_t *write_varint(uint8_t *out, uint64_t value)
{
    while (value >= 0x80) {
        *out++ = (uint8_t)((value & 0x7f) | 0x80);
        value >>= 7;
    }
    *out++ = (uint8_t)value;
    return out;
}

zerobus_status_t
zb_proto_encode_ingest_record(zerobus_offset_t offset_id,
                              zerobus_string_view_t json_record,
                              uint8_t **out_data, size_t *out_len)
{
    if (!check_args(json_record, out_data, out_len)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    /* Update the sizes and encoding below whenever these fields change in
     * rust/sdk/zerobus_service.proto:
     *   EphemeralStreamRequest.ingest_record
     *   IngestRecordRequest.offset_id
     *   IngestRecordRequest.json_record */
    uint64_t offset = (uint64_t)offset_id;
    size_t ingest_len = 1 + varint_size(offset) + 1 +
                        varint_size(json_record.len) + json_record.len;
    size_t len = 1 + varint_size(ingest_len) + ingest_len;
    uint8_t *data = (uint8_t *)malloc(len);
    if (data == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }

    uint8_t *p = data;
    *p++ = 0x12; /* EphemeralStreamRequest.ingest_record: field 2, message. */
    p = write_varint(p, ingest_len);
    *p++ = 0x08; /* IngestRecordRequest.offset_id: field 1, int64. */
    p = write_varint(p, offset);
    *p++ = 0x1a; /* IngestRecordRequest.json_record: field 3, string. */
    p = write_varint(p, json_record.len);
    if (json_record.len > 0) {
        memcpy(p, json_record.data, json_record.len);
    }
    *out_data = data;
    *out_len = len;
    return ZEROBUS_STATUS_OK;
}

static void *unpack_alloc(void *allocator_data, size_t size)
{
    /* A NULL result for malloc(0) does not necessarily mean allocation failed.
     */
    void *data = malloc(size == 0 ? 1 : size);
    if (data == NULL) {
        *(bool *)allocator_data = true;
    }
    return data;
}

static void unpack_free(void *allocator_data, void *data)
{
    (void)allocator_data;
    free(data);
}

zerobus_status_t zb_proto_decode_response(const uint8_t *data, size_t len,
                                          zb_response_t *out_response)
{
    if (out_response == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    memset(out_response, 0, sizeof(*out_response));
    if (data == NULL && len > 0) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }

    static const uint8_t empty = 0;
    const uint8_t *input = data == NULL ? &empty : data;
    bool allocation_failed = false;
    ProtobufCAllocator allocator = {.alloc = unpack_alloc,
                                    .free = unpack_free,
                                    .allocator_data = &allocation_failed};
    Databricks__Zerobus__EphemeralStreamResponse *response =
        databricks__zerobus__ephemeral_stream_response__unpack(&allocator, len,
                                                               input);
    if (response == NULL) {
        return allocation_failed ? ZEROBUS_STATUS_OUT_OF_MEMORY
                                 : ZEROBUS_STATUS_INVALID_ARGUMENT;
    }

    zerobus_status_t status = ZEROBUS_STATUS_OK;
    switch (response->payload_case) {
    case DATABRICKS__ZEROBUS__EPHEMERAL_STREAM_RESPONSE__PAYLOAD_CREATE_STREAM_RESPONSE:
        out_response->kind = ZB_RESPONSE_CREATE_STREAM;
        if (response->create_stream_response->stream_id != NULL) {
            out_response->payload.create_stream.stream_id =
                zb_strdup(response->create_stream_response->stream_id);
            if (out_response->payload.create_stream.stream_id == NULL) {
                status = ZEROBUS_STATUS_OUT_OF_MEMORY;
            }
        }
        break;
    case DATABRICKS__ZEROBUS__EPHEMERAL_STREAM_RESPONSE__PAYLOAD_INGEST_RECORD_RESPONSE:
        out_response->kind = ZB_RESPONSE_INGEST_RECORD;
        out_response->payload.ingest_record.has_durability_ack_up_to_offset =
            response->ingest_record_response->has_durability_ack_up_to_offset;
        out_response->payload.ingest_record.durability_ack_up_to_offset =
            response->ingest_record_response->durability_ack_up_to_offset;
        break;
    case DATABRICKS__ZEROBUS__EPHEMERAL_STREAM_RESPONSE__PAYLOAD_CLOSE_STREAM_SIGNAL:
        out_response->kind = ZB_RESPONSE_CLOSE_STREAM_SIGNAL;
        if (response->close_stream_signal->duration != NULL) {
            out_response->payload.close_stream_signal.has_duration = true;
            out_response->payload.close_stream_signal.duration_seconds =
                response->close_stream_signal->duration->seconds;
            out_response->payload.close_stream_signal.duration_nanos =
                response->close_stream_signal->duration->nanos;
        }
        break;
    default:
        break;
    }
    databricks__zerobus__ephemeral_stream_response__free_unpacked(response,
                                                                  &allocator);
    if (status != ZEROBUS_STATUS_OK) {
        zb_response_clear(out_response);
    }
    return status;
}

void zb_response_clear(zb_response_t *response)
{
    if (response == NULL) {
        return;
    }
    if (response->kind == ZB_RESPONSE_CREATE_STREAM) {
        free(response->payload.create_stream.stream_id);
    }
    memset(response, 0, sizeof(*response));
}
