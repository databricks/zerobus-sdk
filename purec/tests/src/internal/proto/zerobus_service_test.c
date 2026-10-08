/*
 * Unit tests for the EphemeralStream codec (proto/zerobus_service.c).
 *
 * Golden bytes come from protoc, run from a directory holding a copy of
 * rust/sdk/zerobus_service.proto:
 *
 *   printf '%s' '<text proto>' | protoc -I . -I /usr/include \
 *     --encode=databricks.zerobus.<Message> zerobus_service.proto | xxd -i
 */
#include <stdlib.h>
#include <string.h>

#include "internal/proto/zerobus_service.h"
#include "test_common.h"

/* ---- helpers ----------------------------------------------------------- */

static bool bytes_equal(const uint8_t *data, size_t len,
                        const uint8_t *expected, size_t expected_len)
{
    return data != NULL && len == expected_len &&
           memcmp(data, expected, len) == 0;
}

static zerobus_status_t decode(const uint8_t *data, size_t len,
                               zb_response_t *out)
{
    return zb_proto_decode_response(data, len, out);
}

/* ---- tests ------------------------------------------------------------- */

static void test_encode_create_stream(void)
{
    /* create_stream { table_name: "main.default.events" record_type: JSON } */
    static const uint8_t expected[] = {0x0a, 0x17, 0x0a, 0x13, 0x6d, 0x61, 0x69,
                                       0x6e, 0x2e, 0x64, 0x65, 0x66, 0x61, 0x75,
                                       0x6c, 0x74, 0x2e, 0x65, 0x76, 0x65, 0x6e,
                                       0x74, 0x73, 0x20, 0x02};
    uint8_t *data = NULL;
    size_t len = 0;
    CHECK_OK(
        zb_proto_encode_create_stream(sv("main.default.events"), &data, &len));
    CHECK(bytes_equal(data, len, expected, sizeof(expected)));
    free(data);
}

static void test_encode_ingest_record(void)
{
    /* ingest_record { offset_id: 0 json_record: "{\"id\":1}" } */
    static const uint8_t first[] = {0x12, 0x0c, 0x08, 0x00, 0x1a, 0x08, 0x7b,
                                    0x22, 0x69, 0x64, 0x22, 0x3a, 0x31, 0x7d};
    /* ingest_record { offset_id: 300 json_record: "{\"id\":2}" } */
    static const uint8_t multibyte_offset[] = {0x12, 0x0d, 0x08, 0xac, 0x02,
                                               0x1a, 0x08, 0x7b, 0x22, 0x69,
                                               0x64, 0x22, 0x3a, 0x32, 0x7d};
    /* ingest_record { offset_id: -1 json_record: "{}" } */
    static const uint8_t negative_offset[] = {
        0x12, 0x0f, 0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0x01, 0x1a, 0x02, 0x7b, 0x7d};
    /* ingest_record { offset_id: 0 json_record: "" } */
    static const uint8_t empty_record[] = {0x12, 0x04, 0x08, 0x00, 0x1a, 0x00};
    uint8_t *data = NULL;
    size_t len = 0;

    CHECK_OK(zb_proto_encode_ingest_record(0, sv("{\"id\":1}"), &data, &len));
    CHECK(bytes_equal(data, len, first, sizeof(first)));
    free(data);
    data = NULL;

    CHECK_OK(zb_proto_encode_ingest_record(300, sv("{\"id\":2}"), &data, &len));
    CHECK(bytes_equal(data, len, multibyte_offset, sizeof(multibyte_offset)));
    free(data);
    data = NULL;

    CHECK_OK(zb_proto_encode_ingest_record(-1, sv("{}"), &data, &len));
    CHECK(bytes_equal(data, len, negative_offset, sizeof(negative_offset)));
    free(data);
    data = NULL;

    CHECK_OK(zb_proto_encode_ingest_record(0, (zerobus_string_view_t){NULL, 0},
                                           &data, &len));
    CHECK(bytes_equal(data, len, empty_record, sizeof(empty_record)));
    free(data);
}

static void test_encode_record_preserves_invalid_utf8(void)
{
    /* 0xff is invalid UTF-8. The encoder preserves it without validating
     * UTF-8 or JSON. Google's encoder produces the same bytes. */
    static const char record[] = "{\"k\":\"\xff\"}";
    static const uint8_t expected[] = {0x12, 0x0d, 0x08, 0x00, 0x1a,
                                       0x09, 0x7b, 0x22, 0x6b, 0x22,
                                       0x3a, 0x22, 0xff, 0x22, 0x7d};
    uint8_t *data = NULL;
    size_t len = 0;

    CHECK_OK(zb_proto_encode_ingest_record(
        0, (zerobus_string_view_t){record, sizeof(record) - 1}, &data, &len));
    CHECK(bytes_equal(data, len, expected, sizeof(expected)));
    free(data);
}

static void test_encode_long_record(void)
{
    /* ingest_record { offset_id: 7 json_record: "{\"k\":\"a...a\"}" } with
     * 200 a's: a 208-byte record, so both lengths take two bytes. */
    static const uint8_t header[] = {0x12, 0xd5, 0x01, 0x08,
                                     0x07, 0x1a, 0xd0, 0x01};
    char record[209];
    memcpy(record, "{\"k\":\"", 6);
    memset(record + 6, 'a', 200);
    memcpy(record + 206, "\"}", 3);
    uint8_t *data = NULL;
    size_t len = 0;

    CHECK_OK(zb_proto_encode_ingest_record(
        7, (zerobus_string_view_t){record, 208}, &data, &len));
    REQUIRE(data != NULL);
    CHECK_EQ_INT(len, sizeof(header) + 208);
    CHECK(len >= sizeof(header) && memcmp(data, header, sizeof(header)) == 0);
    CHECK(len == sizeof(header) + 208 &&
          memcmp(data + sizeof(header), record, 208) == 0);

zb_cleanup:
    free(data);
}

static void test_encode_rejects_invalid_arguments(void)
{
    uint8_t *data = NULL;
    size_t len = 0;
    CHECK_EQ_INT(zb_proto_encode_create_stream(sv("c.s.t"), NULL, &len),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_proto_encode_create_stream(sv("c.s.t"), &data, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_proto_encode_create_stream((zerobus_string_view_t){NULL, 3},
                                               &data, &len),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_proto_encode_create_stream(
                     (zerobus_string_view_t){"x", SIZE_MAX}, &data, &len),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_proto_encode_ingest_record(0, sv("{}"), NULL, &len),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_proto_encode_ingest_record(0, sv("{}"), &data, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_proto_encode_ingest_record(
                     0, (zerobus_string_view_t){NULL, 3}, &data, &len),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_proto_encode_ingest_record(
                     0, (zerobus_string_view_t){"x", SIZE_MAX}, &data, &len),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(data == NULL);
}

static void test_decode_create_stream_response(void)
{
    /* create_stream_response { stream_id: "sid-1" } */
    static const uint8_t bytes[] = {0x0a, 0x07, 0x0a, 0x05, 0x73,
                                    0x69, 0x64, 0x2d, 0x31};
    /* create_stream_response { } */
    static const uint8_t without_id[] = {0x0a, 0x00};
    zb_response_t response;

    CHECK_OK(decode(bytes, sizeof(bytes), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_CREATE_STREAM);
    CHECK(response.payload.create_stream.stream_id != NULL &&
          strcmp(response.payload.create_stream.stream_id, "sid-1") == 0);
    zb_response_clear(&response);
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_NONE);

    CHECK_OK(decode(without_id, sizeof(without_id), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_CREATE_STREAM);
    CHECK(response.payload.create_stream.stream_id == NULL);
    zb_response_clear(&response);
}

static void test_decode_ingest_record_response(void)
{
    /* ingest_record_response { durability_ack_up_to_offset: 42 } */
    static const uint8_t ack[] = {0x12, 0x02, 0x08, 0x2a};
    /* ingest_record_response { durability_ack_up_to_offset: 1099511627776 } */
    static const uint8_t big_ack[] = {0x12, 0x07, 0x08, 0x80, 0x80,
                                      0x80, 0x80, 0x80, 0x20};
    /* ingest_record_response { } */
    static const uint8_t no_ack[] = {0x12, 0x00};
    zb_response_t response;

    CHECK_OK(decode(ack, sizeof(ack), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_INGEST_RECORD);
    CHECK(response.payload.ingest_record.has_durability_ack_up_to_offset);
    CHECK_EQ_INT(response.payload.ingest_record.durability_ack_up_to_offset,
                 42);

    CHECK_OK(decode(big_ack, sizeof(big_ack), &response));
    CHECK_EQ_INT(response.payload.ingest_record.durability_ack_up_to_offset,
                 INT64_C(1099511627776));

    CHECK_OK(decode(no_ack, sizeof(no_ack), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_INGEST_RECORD);
    CHECK(!response.payload.ingest_record.has_durability_ack_up_to_offset);
}

static void test_decode_close_stream_signal(void)
{
    /* close_stream_signal { duration { seconds: 300 nanos: 500 } } */
    static const uint8_t signal[] = {0x1a, 0x08, 0x0a, 0x06, 0x08,
                                     0xac, 0x02, 0x10, 0xf4, 0x03};
    /* close_stream_signal { duration { seconds: -2 nanos: -1 } } */
    static const uint8_t negative[] = {0x1a, 0x18, 0x0a, 0x16, 0x08, 0xfe, 0xff,
                                       0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                                       0x01, 0x10, 0xff, 0xff, 0xff, 0xff, 0xff,
                                       0xff, 0xff, 0xff, 0xff, 0x01};
    /* close_stream_signal { } */
    static const uint8_t no_duration[] = {0x1a, 0x00};
    zb_response_t response;

    CHECK_OK(decode(signal, sizeof(signal), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_CLOSE_STREAM_SIGNAL);
    CHECK(response.payload.close_stream_signal.has_duration);
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_seconds, 300);
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_nanos, 500);

    CHECK_OK(decode(negative, sizeof(negative), &response));
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_seconds, -2);
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_nanos, -1);

    CHECK_OK(decode(no_duration, sizeof(no_duration), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_CLOSE_STREAM_SIGNAL);
    CHECK(!response.payload.close_stream_signal.has_duration);
}

static void test_decode_empty_message_is_none(void)
{
    static const uint8_t bytes[] = {0x00};
    zb_response_t response;
    CHECK_OK(decode(NULL, 0, &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_NONE);
    CHECK_OK(decode(bytes, 0, &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_NONE);
}

static void test_decode_skips_unknown_fields(void)
{
    /* create_stream_response { stream_id: "sid-1" } with an unknown varint
     * field 2 inside, followed by top-level field 15 as a varint, fixed64,
     * length-delimited value, and fixed32. */
    static const uint8_t bytes[] = {
        0x0a, 0x09, 0x0a, 0x05, 's',  'i', 'd', '-', '1', 0x10, 0x07,
        0x78, 0x01, 0x79, 1,    2,    3,   4,   5,   6,   7,    8,
        0x7a, 0x02, 'x',  'y',  0x7d, 1,   2,   3,   4};
    /* A future oneof member (field 4) is unknown to this decoder. */
    static const uint8_t unknown_member[] = {0x22, 0x01, 0x00};
    /* The ack of 42 and the 300 s 500 ns close signal, each with an unknown
     * varint field 2 inside. */
    static const uint8_t ack[] = {0x12, 0x04, 0x08, 0x2a, 0x10, 0x01};
    static const uint8_t signal[] = {0x1a, 0x0a, 0x10, 0x01, 0x0a, 0x06,
                                     0x08, 0xac, 0x02, 0x10, 0xf4, 0x03};
    zb_response_t response;

    CHECK_OK(decode(bytes, sizeof(bytes), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_CREATE_STREAM);
    CHECK(response.payload.create_stream.stream_id != NULL &&
          strcmp(response.payload.create_stream.stream_id, "sid-1") == 0);
    zb_response_clear(&response);

    CHECK_OK(decode(ack, sizeof(ack), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_INGEST_RECORD);
    CHECK_EQ_INT(response.payload.ingest_record.durability_ack_up_to_offset,
                 42);
    CHECK_OK(decode(signal, sizeof(signal), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_CLOSE_STREAM_SIGNAL);
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_seconds, 300);
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_nanos, 500);

    CHECK_OK(decode(unknown_member, sizeof(unknown_member), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_NONE);
}

static void test_decode_repeated_stream_id_uses_last_value(void)
{
    /* Two create_stream_response members: the later stream_id wins. */
    static const uint8_t created_twice[] = {0x0a, 0x03, 0x0a, 0x01, 'a',
                                            0x0a, 0x03, 0x0a, 0x01, 'b'};
    zb_response_t response;

    CHECK_OK(decode(created_twice, sizeof(created_twice), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_CREATE_STREAM);
    CHECK(response.payload.create_stream.stream_id != NULL &&
          strcmp(response.payload.create_stream.stream_id, "b") == 0);
    zb_response_clear(&response);
}

static void test_decode_last_member_wins(void)
{
    /* create_stream_response { stream_id: "sid-1" }, then an ack of 42. */
    static const uint8_t bytes[] = {0x0a, 0x07, 0x0a, 0x05, 's',  'i', 'd',
                                    '-',  '1',  0x12, 0x02, 0x08, 0x2a};
    zb_response_t response;
    CHECK_OK(decode(bytes, sizeof(bytes), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_INGEST_RECORD);
    CHECK_EQ_INT(response.payload.ingest_record.durability_ack_up_to_offset,
                 42);
    zb_response_clear(&response);
}

static void test_decode_rejects_malformed(void)
{
    static const uint8_t truncated_tag[] = {0x80};
    static const uint8_t field_zero[] = {0x02, 0x00};
    static const uint8_t start_group[] = {0x0b};
    static const uint8_t end_group[] = {0x0c};
    static const uint8_t type_six[] = {0x0e};
    static const uint8_t type_seven[] = {0x0f};
    static const uint8_t length_overrun[] = {0x0a, 0x05, 0x01};
    static const uint8_t nested_overrun[] = {0x0a, 0x02, 0x0a, 0x05};
    static const uint8_t truncated_ack[] = {0x12, 0x02, 0x08, 0x80};
    static const uint8_t overlong_ack[] = {0x12, 0x0c, 0x08, 0xff, 0xff,
                                           0xff, 0xff, 0xff, 0xff, 0xff,
                                           0xff, 0xff, 0xff, 0x01};
    static const uint8_t bad_duration[] = {0x1a, 0x02, 0x0a, 0x05};
    static const uint8_t bad_duration_field[] = {0x1a, 0x03, 0x0a, 0x01, 0x0b};
    static const uint8_t bad_unknown[] = {0x7a, 0x05};
    static const uint8_t bad_unknown_in_ack[] = {0x12, 0x02, 0x10, 0x80};
    static const uint8_t bad_unknown_in_signal[] = {0x1a, 0x02, 0x10, 0x80};
    static const uint8_t truncated_seconds[] = {0x1a, 0x03, 0x0a, 0x01, 0x08};
    static const struct {
        const uint8_t *data;
        size_t len;
    } cases[] = {
        {truncated_tag, sizeof(truncated_tag)},
        {field_zero, sizeof(field_zero)},
        {start_group, sizeof(start_group)},
        {end_group, sizeof(end_group)},
        {type_six, sizeof(type_six)},
        {type_seven, sizeof(type_seven)},
        {length_overrun, sizeof(length_overrun)},
        {nested_overrun, sizeof(nested_overrun)},
        {truncated_ack, sizeof(truncated_ack)},
        {overlong_ack, sizeof(overlong_ack)},
        {bad_duration, sizeof(bad_duration)},
        {bad_duration_field, sizeof(bad_duration_field)},
        {bad_unknown, sizeof(bad_unknown)},
        {bad_unknown_in_ack, sizeof(bad_unknown_in_ack)},
        {bad_unknown_in_signal, sizeof(bad_unknown_in_signal)},
        {truncated_seconds, sizeof(truncated_seconds)},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        zb_response_t response;
        CHECK_EQ_INT(decode(cases[i].data, cases[i].len, &response),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
        CHECK_EQ_INT(response.kind, ZB_RESPONSE_NONE);
    }
}

static void test_decode_malformed_tail_releases_decoded_fields(void)
{
    /* A valid create_stream_response, then a start-group tag. */
    static const uint8_t bytes[] = {0x0a, 0x07, 0x0a, 0x05, 's',
                                    'i',  'd',  '-',  '1',  0x0b};
    /* An unknown field inside create_stream_response that is malformed. */
    static const uint8_t bad_inner[] = {0x0a, 0x02, 0x10, 0x80};
    /* A stream_id whose length runs past its message. */
    static const uint8_t bad_stream_id[] = {0x0a, 0x02, 0x0a, 0x05};
    zb_response_t response;
    CHECK_EQ_INT(decode(bytes, sizeof(bytes), &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_NONE);
    CHECK_EQ_INT(decode(bad_inner, sizeof(bad_inner), &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(decode(bad_stream_id, sizeof(bad_stream_id), &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
}

static void test_decode_rejects_invalid_arguments(void)
{
    zb_response_t response = {
        .kind = ZB_RESPONSE_CLOSE_STREAM_SIGNAL,
        .payload.close_stream_signal = {
            .has_duration = true, .duration_seconds = 1, .duration_nanos = 2}};
    CHECK_EQ_INT(decode(NULL, 3, &response), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_NONE);
    CHECK(!response.payload.close_stream_signal.has_duration);
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_seconds, 0);
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_nanos, 0);
    CHECK_EQ_INT(decode(NULL, 0, NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
}

static void test_clear_is_null_safe(void)
{
    zb_response_clear(NULL);
}

/* ---- Known protobuf-c compatibility behavior --------------------------- */

/*
 * These tests document accepted edge cases where our codec differs from
 * Google's protobuf implementation (checked with protoc --encode/--decode).
 * Explicit wire bytes in decoder tests keep the duplicate fields and unusual
 * wire types visible.
 *
 * The assertions record behavior observed with protobuf-c 1.3.3 and 1.5.2;
 * those versions are not pinned. If a dependency update changes a result,
 * revisit the assertion rather than restoring the limitation.
 */

static void test_compat_repeated_oneof_message_replaces_previous(void)
{
    /*
     * Each input is ONE EphemeralStreamResponse containing two occurrences
     * of the SAME message-valued oneof member. Protobuf's merge rule combines
     * those nested messages; only switching to a DIFFERENT member should
     * discard the previous message. See:
     * https://protobuf.dev/programming-guides/encoding/#last-one-wins
     *
     * Here protobuf-c replaces the whole message with its last occurrence.
     */
    /*
     * The envelope contains two create_stream_response fields:
     * first { stream_id: "a" }, then {}. Google merges the empty second
     * response into the first and keeps "a". Our codec returns only the
     * empty second response, with no stream_id.
     */
    static const uint8_t created_then_empty[] = {0x0a, 0x03, 0x0a, 0x01,
                                                 'a',  0x0a, 0x00};
    /*
     * The envelope contains two ingest_record_response fields:
     * first { durability_ack_up_to_offset: 5 }, then {}. Google merges them
     * and keeps the ack present with offset 5. Our codec returns only the
     * empty second response, so the ack is absent.
     */
    static const uint8_t ack_then_empty[] = {0x12, 0x02, 0x08,
                                             0x05, 0x12, 0x00};
    /*
     * The envelope contains two close_stream_signal fields:
     * first { duration { seconds: 1 } }, then { duration { nanos: 2 } }.
     * Each close signal contains ONE duration. Google merges both the close
     * signals and their durations into (1, 2). Our codec returns only the
     * second close signal, whose duration is (0, 2).
     */
    static const uint8_t split_duration[] = {
        0x1a, 0x04, 0x0a, 0x02, 0x08, 0x01, 0x1a, 0x04, 0x0a, 0x02, 0x10, 0x02};
    zb_response_t response;

    CHECK_OK(decode(created_then_empty, sizeof(created_then_empty), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_CREATE_STREAM);
    CHECK(response.payload.create_stream.stream_id == NULL);
    zb_response_clear(&response);

    CHECK_OK(decode(ack_then_empty, sizeof(ack_then_empty), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_INGEST_RECORD);
    CHECK(!response.payload.ingest_record.has_durability_ack_up_to_offset);
    zb_response_clear(&response);

    CHECK_OK(decode(split_duration, sizeof(split_duration), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_CLOSE_STREAM_SIGNAL);
    CHECK(response.payload.close_stream_signal.has_duration);
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_seconds, 0);
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_nanos, 2);
    zb_response_clear(&response);
}

static void test_compat_repeated_duration_loses_earlier_fields(void)
{
    /*
     * ONE close_stream_signal contains two occurrences of its duration field:
     * first { seconds: 1 }, then { nanos: 2 }. This exercises merging an
     * ordinary singular message field, independently of the outer oneof.
     * Google merges the fields into (1, 2). With protobuf-c the absent seconds
     * in the second Duration overwrite the earlier value with zero, so our
     * codec returns (0, 2).
     */
    static const uint8_t bytes[] = {0x1a, 0x08, 0x0a, 0x02, 0x08,
                                    0x01, 0x0a, 0x02, 0x10, 0x02};
    zb_response_t response;

    CHECK_OK(decode(bytes, sizeof(bytes), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_CLOSE_STREAM_SIGNAL);
    CHECK(response.payload.close_stream_signal.has_duration);
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_seconds, 0);
    CHECK_EQ_INT(response.payload.close_stream_signal.duration_nanos, 2);
    zb_response_clear(&response);
}

static void test_compat_encode_record_with_embedded_nul_is_rejected(void)
{
    /*
     * Attempt to encode one EphemeralStreamRequest whose ingest_record has
     * offset_id 0 and json_record containing three bytes: 'a', NUL, 'b'.
     * The string view includes all three bytes.
     *
     * Google encodes all three bytes in the protobuf string. Protobuf-c
     * measures strings with strlen and would encode only "a". Our check_args
     * rejects the input before packing and returns INVALID_ARGUMENT.
     *
     * A raw NUL is invalid JSON. Inside a JSON string it must be escaped as
     * \u0000; those six ASCII bytes contain no NUL and pass this check.
     */
    static const char record[] = {'a', '\0', 'b'};
    uint8_t *data = NULL;
    size_t len = 0;

    CHECK_EQ_INT(
        zb_proto_encode_ingest_record(
            0, (zerobus_string_view_t){record, sizeof(record)}, &data, &len),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(data == NULL);
    CHECK_EQ_INT(len, 0);
    free(data);
}

static void test_compat_embedded_nul_truncates_stream_id(void)
{
    /*
     * One EphemeralStreamResponse contains one create_stream_response.
     * Its stream_id field declares a three-byte string: 'a', NUL, 'b'.
     * The encoded length includes all three bytes, including the NUL.
     *
     * Embedded NUL is valid in a protobuf string, and Google preserves all
     * three bytes. Protobuf-c exposes the string as char * without a length.
     * Our codec copies it with zb_strdup, which stops at the first NUL, so
     * the returned ID is "a".
     */
    static const uint8_t bytes[] = {0x0a, 0x05, 0x0a, 0x03, 'a', 0x00, 'b'};
    zb_response_t response;

    CHECK_OK(decode(bytes, sizeof(bytes), &response));
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_CREATE_STREAM);
    CHECK(response.payload.create_stream.stream_id != NULL &&
          strcmp(response.payload.create_stream.stream_id, "a") == 0);
    zb_response_clear(&response);
}

static void test_compat_unknown_group_is_rejected(void)
{
    /*
     * One EphemeralStreamResponse contains ingest_record_response
     * { durability_ack_up_to_offset: 42 }, followed by an unknown field 15
     * at the envelope level. Field 15 is encoded as an empty group: its
     * start tag 0x7b is immediately followed by the matching end tag 0x7c.
     *
     * Groups are deprecated but valid wire data. Google accepts the message
     * and retains ack 42. Protobuf-c rejects the group, so our codec returns
     * INVALID_ARGUMENT and leaves the response empty.
     */
    static const uint8_t bytes[] = {0x12, 0x02, 0x08, 0x2a, 0x7b, 0x7c};
    zb_response_t response;

    CHECK_EQ_INT(decode(bytes, sizeof(bytes), &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_NONE);
    zb_response_clear(&response);
}

static void test_compat_known_field_with_other_wire_type_is_rejected(void)
{
    /*
     * One EphemeralStreamResponse contains ingest_record_response
     * { durability_ack_up_to_offset: 42 }, followed by field 1 at the
     * envelope level, encoded as varint 1. That field number belongs to
     * create_stream_response, which expects a length-delimited message.
     * The wire data is complete, but this occurrence has a different type
     * from the one declared in the schema.
     *
     * Google treats that occurrence as unknown and retains ack 42.
     * Protobuf-c rejects it, so our codec returns INVALID_ARGUMENT and
     * leaves the response empty.
     */
    static const uint8_t bytes[] = {0x12, 0x02, 0x08, 0x2a, 0x08, 0x01};
    zb_response_t response;

    CHECK_EQ_INT(decode(bytes, sizeof(bytes), &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(response.kind, ZB_RESPONSE_NONE);
    zb_response_clear(&response);
}

int main(void)
{
    test_encode_create_stream();
    test_encode_ingest_record();
    test_encode_record_preserves_invalid_utf8();
    test_encode_long_record();
    test_encode_rejects_invalid_arguments();
    test_decode_create_stream_response();
    test_decode_ingest_record_response();
    test_decode_close_stream_signal();
    test_decode_empty_message_is_none();
    test_decode_skips_unknown_fields();
    test_decode_repeated_stream_id_uses_last_value();
    test_decode_last_member_wins();
    test_decode_rejects_malformed();
    test_decode_malformed_tail_releases_decoded_fields();
    test_decode_rejects_invalid_arguments();
    test_clear_is_null_safe();

    test_compat_repeated_oneof_message_replaces_previous();
    test_compat_repeated_duration_loses_earlier_fields();
    test_compat_encode_record_with_embedded_nul_is_rejected();
    test_compat_embedded_nul_truncates_stream_id();
    test_compat_unknown_group_is_rejected();
    test_compat_known_field_with_other_wire_type_is_rejected();
    TEST_MAIN_RETURN();
}
