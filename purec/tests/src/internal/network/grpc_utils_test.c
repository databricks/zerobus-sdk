/* Unit tests for network/grpc_utils.c: gRPC message framing, status mapping,
 * and header checks. */
#include <stdlib.h>
#include <string.h>

#include "internal/network/grpc_utils.h"
#include "test_common.h"

/* ---- helpers ----------------------------------------------------------- */

enum { MAXIMUM = 16 }; /* max_message_bytes for the decoder tests */

/* gRPC messages "xx" and "y". */
static const uint8_t xx_y[] = {0, 0, 0, 0, 2, 'x', 'x', 0, 0, 0, 0, 1, 'y'};

typedef struct {
    int refuse; /* refuse this many offers, then accept */
    int offers;
    int delivered;
    char received[64]; /* delivered payloads, concatenated */
} sink_t;

static bool deliver(void *context, zb_message_t *message)
{
    sink_t *sink = context;
    sink->offers++;
    if (sink->refuse > 0) {
        sink->refuse--;
        return false;
    }
    size_t used = strlen(sink->received);
    if (message->len != 0 && message->len < sizeof(sink->received) - used) {
        memcpy(sink->received + used, message->data, message->len);
    }
    sink->delivered++;
    free(message->data);
    free(message);
    return true;
}

/* Feeds one chunk to the decoder. */
static zerobus_status_t feed(zb_grpc_decoder_t *decoder, sink_t *sink,
                             const uint8_t *data, size_t len, size_t *consumed,
                             bool *paused)
{
    return zb_grpc_decoder_feed(decoder, data, len, MAXIMUM, deliver, sink,
                                consumed, paused);
}

/* Percent-decodes s into buffer, which must hold strlen(s) + 1 bytes. */
static const char *decoded(char *buffer, const char *s)
{
    size_t len = strlen(s);
    memcpy(buffer, s, len);
    zb_grpc_percent_decode(buffer, len);
    return buffer;
}

static bool metadata_ok(const char *name, const char *value)
{
    return zb_is_valid_grpc_metadata(sv(name), sv(value));
}

/* ---- message framing --------------------------------------------------- */

static void test_write_prefix(void)
{
    const uint8_t small[] = {0, 1, 2, 3, 4};
    const uint8_t largest[] = {0, 0xff, 0xff, 0xff, 0xff};
    uint8_t prefix[5];

    /* An uncompressed flag, then the length in big-endian order. */
    zb_write_grpc_prefix(prefix, 0x01020304);
    CHECK(memcmp(prefix, small, sizeof(small)) == 0);
    zb_write_grpc_prefix(prefix, UINT32_MAX);
    CHECK(memcmp(prefix, largest, sizeof(largest)) == 0);
}

static void test_decoder_chunks(void)
{
    const uint8_t empty[] = {0, 0, 0, 0, 0};
    zb_grpc_decoder_t decoder = {0};
    sink_t sink = {0};
    size_t consumed = 0;
    bool paused = true;

    /* Whole messages in one chunk are delivered in order. */
    CHECK_OK(feed(&decoder, &sink, xx_y, sizeof(xx_y), &consumed, &paused));
    CHECK(strcmp(sink.received, "xxy") == 0);
    CHECK_EQ_INT(consumed, sizeof(xx_y));
    CHECK(!paused);
    CHECK(zb_grpc_decoder_between_messages(&decoder));

    /* So are messages split across chunks, here one byte per chunk: the
     * decoder is between messages only when one has just completed. */
    sink = (sink_t){0};
    for (size_t i = 0; i < sizeof(xx_y); ++i) {
        CHECK_OK(feed(&decoder, &sink, &xx_y[i], 1, &consumed, &paused));
        CHECK_EQ_INT(consumed, 1);
        CHECK_EQ_INT(zb_grpc_decoder_between_messages(&decoder),
                     i == 6 || i == 12);
    }
    CHECK(strcmp(sink.received, "xxy") == 0);
    CHECK_EQ_INT(sink.delivered, 2);

    /* An empty message is delivered too. */
    sink = (sink_t){0};
    CHECK_OK(feed(&decoder, &sink, empty, sizeof(empty), &consumed, &paused));
    CHECK_EQ_INT(sink.delivered, 1);
    zb_grpc_decoder_clear(&decoder);
}

static void test_decoder_pause(void)
{
    zb_grpc_decoder_t decoder = {0};
    sink_t sink = {.refuse = 1};
    size_t consumed = 0;
    bool paused = false;

    /* A refused message stops the decoder right after it. The next call, with
     * the rest of the chunk, offers it again first. */
    CHECK_OK(feed(&decoder, &sink, xx_y, sizeof(xx_y), &consumed, &paused));
    CHECK(paused);
    CHECK_EQ_INT(consumed, 7);
    CHECK_EQ_INT(sink.delivered, 0);
    CHECK_OK(
        feed(&decoder, &sink, xx_y + 7, sizeof(xx_y) - 7, &consumed, &paused));
    CHECK(!paused);
    CHECK(strcmp(sink.received, "xxy") == 0);
    CHECK_EQ_INT(sink.offers, 3);

    /* Refused at the end of a chunk, and again on the next offer: it stays
     * kept until an offer, even one with no input, is accepted. */
    sink = (sink_t){.refuse = 2};
    CHECK_OK(feed(&decoder, &sink, xx_y, 7, &consumed, &paused));
    CHECK(paused && consumed == 7);
    CHECK_OK(feed(&decoder, &sink, xx_y + 7, 0, &consumed, &paused));
    CHECK(paused && consumed == 0);
    CHECK_OK(feed(&decoder, &sink, xx_y + 7, 0, &consumed, &paused));
    CHECK(!paused);
    CHECK(strcmp(sink.received, "xx") == 0);
    zb_grpc_decoder_clear(&decoder);
}

static void test_decoder_errors(void)
{
    const uint8_t compressed[] = {1, 0, 0, 0, 1, 'x'};
    const uint8_t bad_flag[] = {2, 0, 0, 0, 1, 'x'};
    const uint8_t too_long[] = {0, 0, 0, 0, MAXIMUM + 1};
    const uint8_t then_compressed[] = {0, 0, 0, 0, 1, 'x', 1, 0, 0, 0, 1, 'z'};
    uint8_t largest[5 + MAXIMUM] = {0, 0, 0, 0, MAXIMUM};
    zb_grpc_decoder_t decoder = {0};
    sink_t sink = {0};
    size_t consumed = 0;
    bool paused = false;

    /* Compression is not supported, other flags are invalid, and a message
     * over the maximum is refused (before it is allocated). */
    CHECK_EQ_INT(feed(&decoder, &sink, compressed, sizeof(compressed),
                      &consumed, &paused),
                 ZEROBUS_STATUS_UNIMPLEMENTED);
    zb_grpc_decoder_clear(&decoder);
    CHECK_EQ_INT(
        feed(&decoder, &sink, bad_flag, sizeof(bad_flag), &consumed, &paused),
        ZEROBUS_STATUS_INTERNAL);
    zb_grpc_decoder_clear(&decoder);
    CHECK_EQ_INT(
        feed(&decoder, &sink, too_long, sizeof(too_long), &consumed, &paused),
        ZEROBUS_STATUS_OUT_OF_RANGE);
    zb_grpc_decoder_clear(&decoder);
    CHECK_OK(
        feed(&decoder, &sink, largest, sizeof(largest), &consumed, &paused));
    CHECK_EQ_INT(sink.delivered, 1);

    /* Messages before the bad one are still delivered. */
    sink = (sink_t){0};
    CHECK_EQ_INT(feed(&decoder, &sink, then_compressed, sizeof(then_compressed),
                      &consumed, &paused),
                 ZEROBUS_STATUS_UNIMPLEMENTED);
    CHECK(strcmp(sink.received, "x") == 0);
    zb_grpc_decoder_clear(&decoder);
}

static void test_decoder_clear(void)
{
    zb_grpc_decoder_t decoder = {0};
    sink_t sink = {0};
    size_t consumed = 0;
    bool paused = false;

    /* Clearing frees a partly received message and a refused one (the
     * sanitizer build checks for leaks), and the decoder starts afresh. */
    CHECK_OK(feed(&decoder, &sink, xx_y, 6, &consumed, &paused));
    CHECK(!zb_grpc_decoder_between_messages(&decoder));
    zb_grpc_decoder_clear(&decoder);
    CHECK(zb_grpc_decoder_between_messages(&decoder));
    sink.refuse = 1;
    CHECK_OK(feed(&decoder, &sink, xx_y, 7, &consumed, &paused));
    CHECK(paused);
    zb_grpc_decoder_clear(&decoder);
    CHECK_OK(feed(&decoder, &sink, xx_y, sizeof(xx_y), &consumed, &paused));
    CHECK(strcmp(sink.received, "xxy") == 0);
}

/* ---- status mapping ---------------------------------------------------- */

static void test_map_grpc_status(void)
{
    CHECK_EQ_INT(zb_map_grpc_status(0), ZEROBUS_STATUS_OK);
    CHECK_EQ_INT(zb_map_grpc_status(1), ZEROBUS_STATUS_CANCELLED);
    CHECK_EQ_INT(zb_map_grpc_status(3), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_map_grpc_status(4), ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK_EQ_INT(zb_map_grpc_status(5), ZEROBUS_STATUS_NOT_FOUND);
    CHECK_EQ_INT(zb_map_grpc_status(7), ZEROBUS_STATUS_PERMISSION_DENIED);
    CHECK_EQ_INT(zb_map_grpc_status(8), ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK_EQ_INT(zb_map_grpc_status(9), ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK_EQ_INT(zb_map_grpc_status(11), ZEROBUS_STATUS_OUT_OF_RANGE);
    CHECK_EQ_INT(zb_map_grpc_status(12), ZEROBUS_STATUS_UNIMPLEMENTED);
    CHECK_EQ_INT(zb_map_grpc_status(13), ZEROBUS_STATUS_INTERNAL);
    CHECK_EQ_INT(zb_map_grpc_status(14), ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zb_map_grpc_status(16), ZEROBUS_STATUS_UNAUTHENTICATED);
    /* Codes without an SDK status, and unknown codes, are UNKNOWN. */
    CHECK_EQ_INT(zb_map_grpc_status(2), ZEROBUS_STATUS_UNKNOWN);
    CHECK_EQ_INT(zb_map_grpc_status(6), ZEROBUS_STATUS_UNKNOWN);
    CHECK_EQ_INT(zb_map_grpc_status(10), ZEROBUS_STATUS_UNKNOWN);
    CHECK_EQ_INT(zb_map_grpc_status(15), ZEROBUS_STATUS_UNKNOWN);
    CHECK_EQ_INT(zb_map_grpc_status(17), ZEROBUS_STATUS_UNKNOWN);
    CHECK_EQ_INT(zb_map_grpc_status(UINT32_MAX), ZEROBUS_STATUS_UNKNOWN);
}

static void test_map_transport_errors(void)
{
    CHECK_EQ_INT(zb_map_http_status(400), ZEROBUS_STATUS_INTERNAL);
    CHECK_EQ_INT(zb_map_http_status(401), ZEROBUS_STATUS_UNAUTHENTICATED);
    CHECK_EQ_INT(zb_map_http_status(403), ZEROBUS_STATUS_PERMISSION_DENIED);
    CHECK_EQ_INT(zb_map_http_status(404), ZEROBUS_STATUS_UNIMPLEMENTED);
    CHECK_EQ_INT(zb_map_http_status(429), ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zb_map_http_status(502), ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zb_map_http_status(503), ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zb_map_http_status(504), ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zb_map_http_status(200), ZEROBUS_STATUS_UNKNOWN);
    CHECK_EQ_INT(zb_map_http_status(500), ZEROBUS_STATUS_UNKNOWN);

    CHECK_EQ_INT(zb_map_http2_error(NGHTTP2_REFUSED_STREAM),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zb_map_http2_error(NGHTTP2_CANCEL), ZEROBUS_STATUS_CANCELLED);
    CHECK_EQ_INT(zb_map_http2_error(NGHTTP2_ENHANCE_YOUR_CALM),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK_EQ_INT(zb_map_http2_error(NGHTTP2_INADEQUATE_SECURITY),
                 ZEROBUS_STATUS_PERMISSION_DENIED);
    CHECK_EQ_INT(zb_map_http2_error(NGHTTP2_PROTOCOL_ERROR),
                 ZEROBUS_STATUS_INTERNAL);

    CHECK_EQ_INT(zb_map_nghttp2_error(NGHTTP2_ERR_NOMEM),
                 ZEROBUS_STATUS_OUT_OF_MEMORY);
    CHECK_EQ_INT(zb_map_nghttp2_error(NGHTTP2_ERR_STREAM_ID_NOT_AVAILABLE),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zb_map_nghttp2_error(NGHTTP2_ERR_INVALID_ARGUMENT),
                 ZEROBUS_STATUS_INTERNAL);
}

/* ---- header values ----------------------------------------------------- */

static void test_content_type(void)
{
    const char *const accepted[] = {
        "application/grpc", "application/grpc+proto",
        "application/grpc;charset=utf-8", "Application/GRPC"};
    const char *const rejected[] = {"application/grpcx", "application/grpc-web",
                                    "application/grp", "text/html", ""};
    for (size_t i = 0; i < sizeof(accepted) / sizeof(accepted[0]); ++i) {
        CHECK(zb_is_grpc_content_type((const uint8_t *)accepted[i],
                                      strlen(accepted[i])));
    }
    for (size_t i = 0; i < sizeof(rejected) / sizeof(rejected[0]); ++i) {
        CHECK(!zb_is_grpc_content_type((const uint8_t *)rejected[i],
                                       strlen(rejected[i])));
    }
}

static void test_percent_decode(void)
{
    char buffer[32];
    char with_nul[] = {'a', '\0', 'b', '\0'};

    CHECK(strcmp(decoded(buffer, "all%20good"), "all good") == 0);
    CHECK(strcmp(decoded(buffer, "%4a%4A"), "JJ") == 0);
    CHECK(strcmp(decoded(buffer, ""), "") == 0);
    /* Malformed escapes stay literal, as gRPC requires. */
    CHECK(strcmp(decoded(buffer, "%zz%4"), "%zz%4") == 0);
    CHECK(strcmp(decoded(buffer, "%%41%"), "%A%") == 0);
    /* A NUL, decoded or literal, cannot end the string early. */
    CHECK(strcmp(decoded(buffer, "a%00b"), "a?b") == 0);
    zb_grpc_percent_decode(with_nul, 3);
    CHECK(strcmp(with_nul, "a?b") == 0);
}

static void test_header_bytes(void)
{
    size_t total = 0;

    /* Each field costs its name, its value and 32 bytes, up to the limit. */
    CHECK(zb_add_http2_header_bytes(&total, 4, 6, 100));
    CHECK_EQ_INT(total, 42);
    CHECK(zb_add_http2_header_bytes(&total, 4, 6, 100));
    CHECK_EQ_INT(total, 84);
    CHECK(!zb_add_http2_header_bytes(&total, 4, 6, 100));
    CHECK_EQ_INT(total, 84); /* a refused field adds nothing */

    /* The limit itself is allowed; even an empty field costs 32. */
    total = 0;
    CHECK(zb_add_http2_header_bytes(&total, 4, 6, 42));
    CHECK(!zb_add_http2_header_bytes(&total, 0, 0, 42));
    total = 0;
    CHECK(!zb_add_http2_header_bytes(&total, 0, 0, 31));

    /* Huge lengths are refused without wrapping around. */
    CHECK(!zb_add_http2_header_bytes(&total, SIZE_MAX, 1, 100));
    CHECK(!zb_add_http2_header_bytes(&total, 1, SIZE_MAX, 100));
    CHECK(!zb_add_http2_header_bytes(&total, SIZE_MAX, SIZE_MAX, SIZE_MAX));
    CHECK_EQ_INT(total, 0);
    total = SIZE_MAX - 10;
    CHECK(!zb_add_http2_header_bytes(&total, 0, 0, SIZE_MAX));
    CHECK(total == SIZE_MAX - 10);
}

/* ---- request headers --------------------------------------------------- */

static void test_valid_target(void)
{
    /* A :path starts with '/' and may hold '?' and '@'. */
    CHECK(zb_is_valid_grpc_target(sv("/a/b"), false));
    CHECK(zb_is_valid_grpc_target(sv("/"), false));
    CHECK(zb_is_valid_grpc_target(sv("/a?b=c@d"), false));
    CHECK(!zb_is_valid_grpc_target(sv("a/b"), false));

    /* An :authority is a bare host[:port]. */
    CHECK(zb_is_valid_grpc_target(sv("localhost:8080"), true));
    CHECK(zb_is_valid_grpc_target(sv("host"), true));
    CHECK(!zb_is_valid_grpc_target(sv("user@host"), true));
    CHECK(!zb_is_valid_grpc_target(sv("host/x"), true));
    CHECK(!zb_is_valid_grpc_target(sv("host?x"), true));

    /* Both are non-empty visible ASCII without '#'. */
    CHECK(!zb_is_valid_grpc_target(sv(""), false));
    CHECK(!zb_is_valid_grpc_target(sv(""), true));
    CHECK(!zb_is_valid_grpc_target((zerobus_string_view_t){NULL, 0}, false));
    CHECK(!zb_is_valid_grpc_target((zerobus_string_view_t){NULL, 0}, true));
    CHECK(!zb_is_valid_grpc_target(sv("/a b"), false));
    CHECK(!zb_is_valid_grpc_target(sv("a b"), true));
    CHECK(!zb_is_valid_grpc_target(sv("/a#b"), false));
    CHECK(!zb_is_valid_grpc_target(sv("a#b"), true));
    CHECK(!zb_is_valid_grpc_target(sv("/a\x7f"), false));
    CHECK(!zb_is_valid_grpc_target(sv("a\x7f"), true));
}

static void test_valid_metadata(void)
{
    const zerobus_string_view_t none = {NULL, 0};

    CHECK(metadata_ok("x-a", "v"));
    CHECK(metadata_ok("authorization", "Bearer token"));
    CHECK(metadata_ok("a_b.c-9", " ~")); /* every allowed name char kind */
    CHECK(metadata_ok("x-a", ""));
    CHECK(zb_is_valid_grpc_metadata(sv("x-a"), none));

    /* Names are lowercase tokens. */
    CHECK(!zb_is_valid_grpc_metadata(none, sv("v")));
    CHECK(!metadata_ok("", "v"));
    CHECK(!metadata_ok("Upper", "v"));
    CHECK(!metadata_ok("x a", "v"));
    CHECK(!metadata_ok("x:a", "v"));

    /* grpc-* names and the headers the transport owns or HTTP/2 forbids are
     * reserved, matched exactly. */
    CHECK(!metadata_ok("grpc-timeout", "1S"));
    CHECK(!metadata_ok("grpc-", "v"));
    CHECK(metadata_ok("grpc", "v"));
    CHECK(!metadata_ok("content-type", "v"));
    CHECK(!metadata_ok("content-length", "v"));
    CHECK(!metadata_ok("te", "v"));
    CHECK(!metadata_ok("host", "v"));
    CHECK(!metadata_ok("connection", "v"));
    CHECK(!metadata_ok("keep-alive", "v"));
    CHECK(!metadata_ok("proxy-connection", "v"));
    CHECK(!metadata_ok("transfer-encoding", "v"));
    CHECK(!metadata_ok("upgrade", "v"));
    CHECK(metadata_ok("tea", "v"));

    /* Values are printable ASCII. */
    CHECK(!metadata_ok("x-a", "a\r\nb"));
    CHECK(!metadata_ok("x-a", "\t"));
    CHECK(!metadata_ok("x-a", "\x7f"));
    CHECK(!zb_is_valid_grpc_metadata(sv("x-a"),
                                     (zerobus_string_view_t){"a\0b", 3}));
    CHECK(!zb_is_valid_grpc_metadata(sv("x-a"),
                                     (zerobus_string_view_t){NULL, 1}));
}

static void test_make_nv(void)
{
    const zerobus_string_view_t name = sv("x-a");
    const zerobus_string_view_t value = sv("v");

    /* The field borrows the header's bytes. */
    nghttp2_nv nv = zb_make_nghttp2_nv(name, value);
    CHECK((const void *)nv.name == (const void *)name.data);
    CHECK((const void *)nv.value == (const void *)value.data);
    CHECK(nv.namelen == 3 && nv.valuelen == 1);
    CHECK_EQ_INT(nv.flags, NGHTTP2_NV_FLAG_NONE);
    /* The credential is never indexed, so HPACK tables do not keep it. */
    CHECK_EQ_INT(
        zb_make_nghttp2_nv(sv("authorization"), sv("Bearer token")).flags,
        NGHTTP2_NV_FLAG_NO_INDEX);
}

int main(void)
{
    test_write_prefix();
    test_decoder_chunks();
    test_decoder_pause();
    test_decoder_errors();
    test_decoder_clear();
    test_map_grpc_status();
    test_map_transport_errors();
    test_content_type();
    test_percent_decode();
    test_header_bytes();
    test_valid_target();
    test_valid_metadata();
    test_make_nv();
    TEST_MAIN_RETURN();
}
