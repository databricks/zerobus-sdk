#include <stdlib.h>
#include <string.h>

#include <nghttp2/nghttp2.h>

#include "internal/network/http2.h"
#include "test_common.h"

/* ---- helpers ---- */

typedef struct {
    char name[64];
    char value[512];
    uint8_t flags;
} peer_header_t;

typedef struct {
    int32_t id;
    peer_header_t headers[16];
    size_t header_count;
    bool end_stream;
} peer_call_t;

typedef struct {
    nghttp2_session *session;
    peer_call_t calls[16];
    size_t call_count;
    size_t settings_count;
    size_t ping_acks;
    size_t resets;
} peer_t;

static peer_call_t *peer_call(peer_t *peer, int32_t id)
{
    for (size_t i = 0; i < peer->call_count; ++i) {
        if (peer->calls[i].id == id) {
            return &peer->calls[i];
        }
    }
    return NULL;
}

static int peer_begin_headers(nghttp2_session *session,
                              const nghttp2_frame *frame, void *user_data)
{
    (void)session;
    peer_t *peer = user_data;
    if (frame->headers.cat != NGHTTP2_HCAT_REQUEST ||
        peer->call_count == sizeof(peer->calls) / sizeof(peer->calls[0])) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    peer->calls[peer->call_count++].id = frame->hd.stream_id;
    return 0;
}

static int peer_header(nghttp2_session *session, const nghttp2_frame *frame,
                       const uint8_t *name, size_t name_len,
                       const uint8_t *value, size_t value_len, uint8_t flags,
                       void *user_data)
{
    (void)session;
    peer_call_t *call = peer_call(user_data, frame->hd.stream_id);
    if (call == NULL ||
        call->header_count ==
            sizeof(call->headers) / sizeof(call->headers[0]) ||
        name_len >= sizeof(call->headers[0].name) ||
        value_len >= sizeof(call->headers[0].value)) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    peer_header_t *header = &call->headers[call->header_count++];
    memcpy(header->name, name, name_len);
    memcpy(header->value, value, value_len);
    header->flags = flags;
    return 0;
}

static int peer_frame(nghttp2_session *session, const nghttp2_frame *frame,
                      void *user_data)
{
    (void)session;
    peer_t *peer = user_data;
    if (frame->hd.type == NGHTTP2_HEADERS) {
        peer_call_t *call = peer_call(peer, frame->hd.stream_id);
        if (call != NULL) {
            call->end_stream = (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0;
        }
    } else if (frame->hd.type == NGHTTP2_SETTINGS &&
               !(frame->hd.flags & NGHTTP2_FLAG_ACK)) {
        ++peer->settings_count;
    } else if (frame->hd.type == NGHTTP2_PING &&
               (frame->hd.flags & NGHTTP2_FLAG_ACK)) {
        ++peer->ping_acks;
    } else if (frame->hd.type == NGHTTP2_RST_STREAM) {
        ++peer->resets;
    }
    return 0;
}

static bool peer_init(peer_t *peer, uint32_t max_calls)
{
    nghttp2_session_callbacks *callbacks = NULL;
    if (nghttp2_session_callbacks_new(&callbacks) != 0) {
        return false;
    }
    nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks,
                                                            peer_begin_headers);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, peer_header);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, peer_frame);
    int result = nghttp2_session_server_new(&peer->session, callbacks, peer);
    nghttp2_session_callbacks_del(callbacks);
    if (result != 0) {
        return false;
    }
    const nghttp2_settings_entry settings = {
        NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, max_calls};
    return nghttp2_submit_settings(peer->session, NGHTTP2_FLAG_NONE, &settings,
                                   1) == 0;
}

static bool send_client(zb_h2_t *connection, peer_t *peer, size_t fragment,
                        bool *moved)
{
    for (size_t turn = 0; turn < 256; ++turn) {
        zb_h2_output_t output;
        if (zb_h2_next_output(connection, &output) != ZEROBUS_STATUS_OK) {
            return false;
        }
        if (output.len == 0) {
            return true;
        }
        for (size_t cursor = 0; cursor < output.len;) {
            size_t len = output.len - cursor;
            if (len > fragment) {
                len = fragment;
            }
            if (nghttp2_session_mem_recv(peer->session, output.data + cursor,
                                         len) != (ssize_t)len) {
                return false;
            }
            cursor += len;
        }
        if (zb_h2_consume_output(connection, output.len) != ZEROBUS_STATUS_OK) {
            return false;
        }
        *moved = true;
    }
    return false;
}

static zerobus_status_t send_peer(peer_t *peer, zb_h2_t *connection,
                                  size_t fragment, bool *moved)
{
    for (size_t turn = 0; turn < 256; ++turn) {
        const uint8_t *data = NULL;
        ssize_t output_len = nghttp2_session_mem_send(peer->session, &data);
        if (output_len < 0) {
            return ZEROBUS_STATUS_INTERNAL;
        }
        if (output_len == 0) {
            return ZEROBUS_STATUS_OK;
        }
        for (size_t cursor = 0; cursor < (size_t)output_len;) {
            size_t len = (size_t)output_len - cursor;
            if (len > fragment) {
                len = fragment;
            }
            size_t consumed = 0;
            zerobus_status_t status =
                zb_h2_feed(connection, data + cursor, len, &consumed);
            if (status != ZEROBUS_STATUS_OK) {
                return status;
            }
            if (consumed != len) {
                return ZEROBUS_STATUS_INTERNAL;
            }
            cursor += consumed;
        }
        *moved = true;
    }
    return ZEROBUS_STATUS_INTERNAL;
}

static bool exchange(zb_h2_t *connection, peer_t *peer, size_t fragment)
{
    for (size_t turn = 0; turn < 32; ++turn) {
        bool moved = false;
        if (!send_client(connection, peer, fragment, &moved) ||
            send_peer(peer, connection, fragment, &moved) !=
                ZEROBUS_STATUS_OK) {
            return false;
        }
        if (!moved) {
            return true;
        }
    }
    return false;
}

static const peer_header_t *find_header(const peer_call_t *call,
                                        const char *name)
{
    for (size_t i = 0; i < call->header_count; ++i) {
        if (strcmp(call->headers[i].name, name) == 0) {
            return &call->headers[i];
        }
    }
    return NULL;
}

static bool header_is(const peer_call_t *call, const char *name,
                      const char *value)
{
    const peer_header_t *header = find_header(call, name);
    return header != NULL && strcmp(header->value, value) == 0;
}

static zb_h2_config_t config_for(size_t max_calls, size_t max_header_bytes)
{
    zb_h2_config_t config = {sv("example.test"), max_calls, max_header_bytes};
    return config;
}

static nghttp2_nv peer_nv(const char *name, const char *value)
{
    nghttp2_nv nv = {(uint8_t *)name, (uint8_t *)value, strlen(name),
                     strlen(value), NGHTTP2_NV_FLAG_NONE};
    return nv;
}

static size_t protocol_header_bytes(const char *path)
{
    return 6 * 32 +
           strlen(":methodPOST:schemehttp:authorityexample.test:path"
                  "content-typeapplication/grpctetrailers") +
           strlen(path);
}

static int near_exhaustion_factory(nghttp2_session **session,
                                   const nghttp2_session_callbacks *callbacks,
                                   void *user_data,
                                   const nghttp2_option *option)
{
    int result =
        nghttp2_session_client_new2(session, callbacks, user_data, option);
    if (result == 0) {
        result = nghttp2_session_set_next_stream_id(*session, INT32_MAX - 2);
    }
    return result;
}

/* ---- tests ---- */

static void test_configuration_validation(void)
{
    zb_h2_t *connection = NULL;
    zb_h2_config_t config = config_for(4, 4096);
    CHECK_EQ_INT(zb_h2_new(NULL, &connection), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_h2_new(&config, NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    const zb_h2_config_t invalid[] = {
        {sv("example.test"), 0, 4096},
        {sv("example.test"), 1, 0},
        {{NULL, 1}, 1, 4096},
        {{NULL, 0}, 1, 4096},
        {sv(""), 1, 4096},
        {sv("example.test"), 1, 1},
        {sv("http://example.test"), 1, 4096},
        {sv("example.test/a"), 1, 4096},
        {sv("example.test?q"), 1, 4096},
        {sv("user@example.test"), 1, 4096},
        {sv("example.test#f"), 1, 4096},
        {sv("example test"), 1, 4096},
        {sv("example\rtest"), 1, 4096},
        {sv("example\xfftest"), 1, 4096},
    };
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
        CHECK_EQ_INT(zb_h2_new(&invalid[i], &connection),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
        CHECK(connection == NULL);
    }
    if (SIZE_MAX > UINT32_MAX) {
        config.max_header_bytes = SIZE_MAX;
        CHECK_EQ_INT(zb_h2_new(&config, &connection),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
    }
    config = config_for(4, 4096);
    config.authority = sv("[::1]:8080");
    REQUIRE_OK(zb_h2_new(&config, &connection));
    CHECK(zb_h2_wants_input(connection));
    CHECK(!zb_h2_wants_input(NULL));
    zb_h2_free(NULL);
zb_cleanup:
    zb_h2_free(connection);
}

static void test_argument_validation(void)
{
    zb_h2_t *connection = NULL;
    zb_h2_config_t config = config_for(4, 4096);
    zb_h2_call_id_t id = 77;
    size_t consumed = 77;
    const uint8_t byte = 0;
    zb_h2_output_t output = {&byte, 77};
    REQUIRE_OK(zb_h2_new(&config, &connection));
    CHECK_EQ_INT(zb_h2_open(NULL, sv("/s/m"), NULL, 0, &id),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_h2_open(connection, sv("/s/m"), NULL, 0, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_h2_open(connection, sv("/s/m"), NULL, 1, &id),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    const zerobus_string_view_t paths[] = {
        {NULL, 0},  {NULL, 3},   sv(""),       sv("no-slash"), sv("/a b"),
        sv("/a#b"), sv("/a\nb"), {"/a\0b", 4}, sv("/a\xff")};
    for (size_t i = 0; i < sizeof(paths) / sizeof(paths[0]); ++i) {
        CHECK_EQ_INT(zb_h2_open(connection, paths[i], NULL, 0, &id),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
        CHECK_EQ_INT(id, 77);
    }
    CHECK_EQ_INT(zb_h2_feed(NULL, NULL, 0, &consumed),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_h2_feed(connection, NULL, 1, &consumed),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_h2_feed(connection, &byte, 1, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(consumed, 77);
    CHECK_OK(zb_h2_feed(connection, NULL, 0, &consumed));
    CHECK_EQ_INT(consumed, 0);
    CHECK_EQ_INT(zb_h2_next_output(NULL, &output),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_h2_next_output(connection, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(output.data == &byte && output.len == 77);
    CHECK_EQ_INT(zb_h2_consume_output(NULL, 0),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_h2_consume_output(connection, 1),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_OK(zb_h2_consume_output(connection, 0));
zb_cleanup:
    zb_h2_free(connection);
}

static void test_metadata_validation(void)
{
    zb_h2_t *connection = NULL;
    zb_h2_config_t config = config_for(4, 4096);
    zb_h2_call_id_t id = 77;
    REQUIRE_OK(zb_h2_new(&config, &connection));
    const char *const names[] = {"",
                                 "Upper",
                                 "has space",
                                 "has!punctuation",
                                 ":method",
                                 "content-type",
                                 "content-length",
                                 "te",
                                 "host",
                                 "connection",
                                 "keep-alive",
                                 "proxy-connection",
                                 "transfer-encoding",
                                 "upgrade",
                                 "grpc-status",
                                 "grpc-timeout",
                                 "grpc-encoding"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
        zb_h2_header_t header = {sv(names[i]), sv("value")};
        CHECK_EQ_INT(zb_h2_open(connection, sv("/s/m"), &header, 1, &id),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
        CHECK_EQ_INT(id, 77);
    }
    const zb_h2_header_t invalid[] = {
        {{NULL, 0}, sv("x")},
        {{NULL, 1}, sv("x")},
        {sv("valid"), {NULL, 1}},
        {sv("valid"), sv("bad\rvalue")},
        {sv("valid"), sv("bad\nvalue")},
        {sv("valid"), {"bad\0value", 9}},
        {sv("valid"), sv("bad\tvalue")},
        {sv("valid"), sv("bad\x7fvalue")},
        {sv("valid"), sv("bad\xffvalue")},
    };
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
        CHECK_EQ_INT(zb_h2_open(connection, sv("/s/m"), &invalid[i], 1, &id),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
        CHECK_EQ_INT(id, 77);
    }
zb_cleanup:
    zb_h2_free(connection);
}

static void test_settings_and_owned_request_headers(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    char authority[] = "example.test";
    char path[] = "/test.Service/Stream";
    char name[] = "x-custom_1.name";
    char value[] = "metadata";
    char authorization[] = "Bearer test-only";
    zb_h2_config_t config = {sv(authority), 4, 4096};
    zb_h2_header_t headers[] = {{sv(name), sv(value)},
                                {sv("authorization"), sv(authorization)},
                                {sv("empty"), {NULL, 0}}};
    zb_h2_call_id_t id = 0;
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 4));
    memset(authority, 'x', sizeof(authority) - 1);
    REQUIRE_OK(zb_h2_open(connection, sv(path), headers, 3, &id));
    memset(path, 'x', sizeof(path) - 1);
    memset(name, 'x', sizeof(name) - 1);
    memset(value, 'x', sizeof(value) - 1);
    memset(authorization, 'x', sizeof(authorization) - 1);
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.settings_count, 1);
    CHECK_EQ_INT(nghttp2_session_get_remote_settings(
                     peer.session, NGHTTP2_SETTINGS_ENABLE_PUSH),
                 0);
    CHECK_EQ_INT(nghttp2_session_get_remote_settings(
                     peer.session, NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE),
                 4096);
    REQUIRE(peer.call_count == 1);
    const peer_call_t *call = &peer.calls[0];
    CHECK_EQ_INT(call->id, id);
    CHECK_EQ_INT(call->header_count, 9);
    CHECK(!call->end_stream);
    CHECK(header_is(call, ":method", "POST"));
    CHECK(header_is(call, ":scheme", "http"));
    CHECK(header_is(call, ":authority", "example.test"));
    CHECK(header_is(call, ":path", "/test.Service/Stream"));
    CHECK(header_is(call, "content-type", "application/grpc"));
    CHECK(header_is(call, "te", "trailers"));
    CHECK(header_is(call, "x-custom_1.name", "metadata"));
    CHECK(header_is(call, "authorization", "Bearer test-only"));
    CHECK(header_is(call, "empty", ""));
    const peer_header_t *auth = find_header(call, "authorization");
    REQUIRE(auth != NULL);
    CHECK((auth->flags & NGHTTP2_NV_FLAG_NO_INDEX) != 0);
    zb_h2_output_t output;
    CHECK_OK(zb_h2_next_output(connection, &output));
    CHECK(output.data == NULL && output.len == 0);
    CHECK(zb_h2_wants_input(connection));
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_call_capacity_and_reset(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    zb_h2_config_t config = config_for(3, 4096);
    zb_h2_call_id_t ids[4] = {0, 0, 0, 77};
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 3));
    for (size_t i = 0; i < 3; ++i) {
        REQUIRE_OK(zb_h2_open(connection, sv("/s/m"), NULL, 0, &ids[i]));
        CHECK_EQ_INT(ids[i], 1 + 2 * i);
    }
    CHECK_EQ_INT(zb_h2_open(connection, sv("/s/m"), NULL, 0, &ids[3]),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK_EQ_INT(ids[3], 77);
    REQUIRE(exchange(connection, &peer, SIZE_MAX));
    REQUIRE(nghttp2_submit_rst_stream(peer.session, NGHTTP2_FLAG_NONE, ids[1],
                                      NGHTTP2_CANCEL) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/m"), NULL, 0, &ids[3]));
    CHECK_EQ_INT(ids[3], 7);
    REQUIRE(exchange(connection, &peer, SIZE_MAX));
    CHECK_EQ_INT(peer.call_count, 4);
    for (size_t i = 0; i < 4; ++i) {
        if (i != 1) {
            REQUIRE(nghttp2_submit_rst_stream(peer.session, NGHTTP2_FLAG_NONE,
                                              ids[i], NGHTTP2_CANCEL) == 0);
        }
    }
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_OK(zb_h2_open(connection, sv("/s/m"), NULL, 0, &ids[0]));
    CHECK_EQ_INT(ids[0], 9);
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_peer_concurrency_queue(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    zb_h2_config_t config = config_for(2, 4096);
    zb_h2_call_id_t first = 0, second = 0, rejected = 77;
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 1));
    REQUIRE(exchange(connection, &peer, 1));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/one"), NULL, 0, &first));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/two"), NULL, 0, &second));
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.call_count, 1);
    CHECK_EQ_INT(peer.calls[0].id, first);
    CHECK_EQ_INT(zb_h2_open(connection, sv("/s/three"), NULL, 0, &rejected),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK_EQ_INT(rejected, 77);
    REQUIRE(nghttp2_submit_rst_stream(peer.session, NGHTTP2_FLAG_NONE, first,
                                      NGHTTP2_CANCEL) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.call_count, 2);
    CHECK_EQ_INT(peer.calls[1].id, second);
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_outbound_header_budget(void)
{
    zb_h2_t *connection = NULL;
    zb_h2_t *small = NULL;
    peer_t peer = {0};
    zb_h2_call_id_t id = 77;
    zb_h2_header_t header = {sv("x-name"), sv("value")};
    size_t exact = protocol_header_bytes("/s/m") + 32 + 6 + 5;
    zb_h2_config_t config = config_for(4, exact);
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 4));
    const zb_h2_header_t huge[] = {{{"a", SIZE_MAX}, sv("")},
                                   {sv("a"), {"x", SIZE_MAX}}};
    for (size_t i = 0; i < sizeof(huge) / sizeof(huge[0]); ++i) {
        CHECK_EQ_INT(zb_h2_open(connection, sv("/s/m"), &huge[i], 1, &id),
                     ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
        CHECK_EQ_INT(id, 77);
    }
    CHECK_EQ_INT(zb_h2_open(connection, sv("/s/m"), &header, SIZE_MAX, &id),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    zerobus_string_view_t huge_path = {"/", SIZE_MAX};
    CHECK_EQ_INT(zb_h2_open(connection, huge_path, NULL, 0, &id),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    header.value = sv("value!");
    CHECK_EQ_INT(zb_h2_open(connection, sv("/s/m"), &header, 1, &id),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK_EQ_INT(id, 77);
    header.value = sv("value");
    REQUIRE_OK(zb_h2_open(connection, sv("/s/m"), &header, 1, &id));
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.call_count, 1);
    config.max_header_bytes = protocol_header_bytes("/s/m") - 1;
    REQUIRE_OK(zb_h2_new(&config, &small));
    id = 77;
    CHECK_EQ_INT(zb_h2_open(small, sv("/s/m"), NULL, 0, &id),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK_EQ_INT(id, 77);
zb_cleanup:
    zb_h2_free(connection);
    zb_h2_free(small);
    nghttp2_session_del(peer.session);
}

static void test_output_retention_while_feeding(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    zb_h2_config_t config = config_for(4, 4096);
    uint8_t snapshot[NGHTTP2_CLIENT_MAGIC_LEN];
    zb_h2_output_t first = {0}, next = {0};
    bool moved = false;
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 4));
    REQUIRE_OK(zb_h2_next_output(connection, &first));
    REQUIRE(first.len == sizeof(snapshot));
    memcpy(snapshot, first.data, sizeof(snapshot));
    CHECK(memcmp(snapshot, NGHTTP2_CLIENT_MAGIC, sizeof(snapshot)) == 0);
    CHECK_OK(zb_h2_next_output(connection, &next));
    CHECK(first.data == next.data && first.len == next.len);
    CHECK_EQ_INT(zb_h2_consume_output(connection, first.len + 1),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    REQUIRE_OK(send_peer(&peer, connection, 1, &moved));
    CHECK(moved);
    CHECK(memcmp(first.data, snapshot, sizeof(snapshot)) == 0);
    REQUIRE(nghttp2_session_mem_recv(peer.session, first.data, 3) == 3);
    CHECK_OK(zb_h2_consume_output(connection, 3));
    CHECK_OK(zb_h2_consume_output(connection, 0));
    CHECK_OK(zb_h2_next_output(connection, &next));
    CHECK(next.data == first.data + 3 && next.len == first.len - 3);
    CHECK(memcmp(next.data, snapshot + 3, next.len) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    REQUIRE_OK(zb_h2_next_output(connection, &next));
    CHECK(next.data == NULL && next.len == 0);
    const uint8_t ping[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    REQUIRE(nghttp2_submit_ping(peer.session, NGHTTP2_FLAG_NONE, ping) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.ping_acks, 1);
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_headers_output_retention_while_feeding(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    uint8_t *snapshot = NULL;
    zb_h2_config_t config = config_for(2, 4096);
    zb_h2_call_id_t first = 0, second = 0;
    zb_h2_output_t output = {0}, remaining = {0};
    zb_h2_header_t header = {sv("x-original"), sv("preserved-value")};
    bool moved = false;
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 2));
    REQUIRE(exchange(connection, &peer, 1));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/original"), &header, 1, &first));
    REQUIRE_OK(zb_h2_next_output(connection, &output));
    REQUIRE(output.len >= 9);
    CHECK_EQ_INT(output.data[3], NGHTTP2_HEADERS);
    snapshot = malloc(output.len);
    REQUIRE(snapshot != NULL);
    memcpy(snapshot, output.data, output.len);

    const size_t prefix = 5;
    REQUIRE(nghttp2_session_mem_recv(peer.session, output.data, prefix) ==
            (ssize_t)prefix);
    REQUIRE_OK(zb_h2_consume_output(connection, prefix));
    const uint8_t ping[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    REQUIRE(nghttp2_submit_ping(peer.session, NGHTTP2_FLAG_NONE, ping) == 0);
    REQUIRE_OK(send_peer(&peer, connection, 1, &moved));
    CHECK(moved);
    REQUIRE_OK(zb_h2_open(connection, sv("/s/second"), NULL, 0, &second));
    REQUIRE_OK(zb_h2_next_output(connection, &remaining));
    CHECK(remaining.data == output.data + prefix);
    REQUIRE(remaining.len == output.len - prefix);
    CHECK(memcmp(remaining.data, snapshot + prefix, remaining.len) == 0);
    REQUIRE(nghttp2_session_mem_recv(peer.session, remaining.data,
                                     remaining.len) == (ssize_t)remaining.len);
    REQUIRE_OK(zb_h2_consume_output(connection, remaining.len));
    REQUIRE(exchange(connection, &peer, 1));
    REQUIRE(peer.call_count == 2);
    CHECK_EQ_INT(peer.calls[0].id, first);
    CHECK(header_is(&peer.calls[0], ":path", "/s/original"));
    CHECK(header_is(&peer.calls[0], "x-original", "preserved-value"));
    CHECK(!peer.calls[0].end_stream);
    CHECK_EQ_INT(peer.calls[1].id, second);
    CHECK(header_is(&peer.calls[1], ":path", "/s/second"));
    CHECK_EQ_INT(peer.ping_acks, 1);
zb_cleanup:
    free(snapshot);
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_fragmented_response_header_blocks(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    zb_h2_config_t config = config_for(1, 512);
    zb_h2_call_id_t id = 0;
    char padding[301];
    memset(padding, 'x', sizeof(padding) - 1);
    padding[sizeof(padding) - 1] = '\0';
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 1));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/m"), NULL, 0, &id));
    REQUIRE(exchange(connection, &peer, 1));
    nghttp2_nv interim[] = {peer_nv(":status", "100"),
                            peer_nv("x-pad", padding)};
    REQUIRE(nghttp2_submit_headers(peer.session, NGHTTP2_FLAG_NONE, id, NULL,
                                   interim, 2, NULL) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    nghttp2_nv response[] = {peer_nv(":status", "200"),
                             peer_nv("content-type", "application/grpc"),
                             peer_nv("x-pad", padding)};
    REQUIRE(nghttp2_submit_headers(peer.session, NGHTTP2_FLAG_NONE, id, NULL,
                                   response, 3, NULL) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    nghttp2_nv trailers[] = {peer_nv("grpc-status", "0")};
    REQUIRE(nghttp2_submit_trailer(peer.session, id, trailers, 1) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    CHECK(zb_h2_wants_input(connection));
    CHECK_EQ_INT(zb_h2_open(connection, sv("/s/m"), NULL, 0, &id),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_inbound_header_budget_preserves_other_calls(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    zb_h2_config_t config = config_for(2, 512);
    zb_h2_call_id_t rejected = 0, surviving = 0, replacement = 0;
    char padding[501];
    memset(padding, 'x', sizeof(padding) - 1);
    padding[sizeof(padding) - 1] = '\0';
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 2));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/rejected"), NULL, 0, &rejected));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/surviving"), NULL, 0, &surviving));
    REQUIRE(exchange(connection, &peer, 1));
    nghttp2_nv response[] = {peer_nv(":status", "200"),
                             peer_nv("content-type", "application/grpc"),
                             peer_nv("x-pad", padding)};
    REQUIRE(nghttp2_submit_response(peer.session, rejected, response, 3,
                                    NULL) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.resets, 1);
    CHECK(zb_h2_wants_input(connection));
    REQUIRE(nghttp2_submit_headers(peer.session, NGHTTP2_FLAG_NONE, surviving,
                                   NULL, response, 2, NULL) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    nghttp2_nv trailers[] = {peer_nv("grpc-status", "0")};
    REQUIRE(nghttp2_submit_trailer(peer.session, surviving, trailers, 1) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.resets, 1);
    REQUIRE_OK(
        zb_h2_open(connection, sv("/s/replacement"), NULL, 0, &replacement));
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.call_count, 3);
    CHECK_EQ_INT(replacement, 5);
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_malformed_input_is_sticky(void)
{
    zb_h2_t *connection = NULL;
    zb_h2_config_t config = config_for(4, 4096);
    size_t consumed = 77;
    zb_h2_call_id_t id = 77;
    zb_h2_output_t output = {NULL, 77};
    /* A peer's first frame must be SETTINGS, not PING. */
    const uint8_t invalid[] = {
        0, 0, 8, NGHTTP2_PING, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    REQUIRE_OK(zb_h2_new(&config, &connection));
    /* Drain the initial preface/SETTINGS before submitting malformed input. */
    do {
        REQUIRE_OK(zb_h2_next_output(connection, &output));
        REQUIRE_OK(zb_h2_consume_output(connection, output.len));
    } while (output.len != 0);
    REQUIRE_OK(zb_h2_feed(connection, invalid, sizeof(invalid), &consumed));
    CHECK_EQ_INT(consumed, sizeof(invalid));
    output.len = 77;
    CHECK_EQ_INT(zb_h2_next_output(connection, &output),
                 ZEROBUS_STATUS_INTERNAL);
    CHECK(output.data == NULL && output.len == 77);
    CHECK(!zb_h2_wants_input(connection));
    consumed = 77;
    CHECK_EQ_INT(zb_h2_feed(connection, NULL, 0, &consumed),
                 ZEROBUS_STATUS_INTERNAL);
    CHECK_EQ_INT(consumed, 77);
    CHECK_EQ_INT(zb_h2_next_output(connection, &output),
                 ZEROBUS_STATUS_INTERNAL);
    CHECK(output.data == NULL && output.len == 77);
    CHECK_EQ_INT(zb_h2_consume_output(connection, 0), ZEROBUS_STATUS_INTERNAL);
    CHECK_EQ_INT(zb_h2_open(connection, sv("/s/m"), NULL, 0, &id),
                 ZEROBUS_STATUS_INTERNAL);
    CHECK_EQ_INT(id, 77);
zb_cleanup:
    zb_h2_free(connection);
}

static void test_invalid_settings_is_fatal(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    zb_h2_config_t config = config_for(1, 4096);
    size_t consumed = 77;
    zb_h2_output_t output = {NULL, 77};
    /* SETTINGS_INITIAL_WINDOW_SIZE cannot exceed 2^31 - 1. */
    const uint8_t invalid[] = {0,
                               0,
                               6,
                               NGHTTP2_SETTINGS,
                               0,
                               0,
                               0,
                               0,
                               0,
                               0,
                               NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE,
                               0x80,
                               0,
                               0,
                               0};
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 1));
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_OK(zb_h2_feed(connection, invalid, sizeof(invalid), &consumed));
    CHECK_EQ_INT(consumed, sizeof(invalid));
    CHECK_EQ_INT(zb_h2_next_output(connection, &output),
                 ZEROBUS_STATUS_INTERNAL);
    CHECK(output.data == NULL && output.len == 77);
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_pending_ack_limit_is_fatal(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    zb_h2_config_t config = config_for(1, 4096);
    const uint8_t ping[] = {0, 0, 8, NGHTTP2_PING, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5,
                            6, 7, 8};
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 1));
    REQUIRE(exchange(connection, &peer, 1));
    /* nghttp2 bounds automatic ACKs at 1000 until output is drained. */
    for (size_t i = 0; i < 1024 && status == ZEROBUS_STATUS_OK; ++i) {
        size_t consumed = 0;
        status = zb_h2_feed(connection, ping, sizeof(ping), &consumed);
    }
    CHECK_EQ_INT(status, ZEROBUS_STATUS_INTERNAL);
    CHECK(!zb_h2_wants_input(connection));
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_stream_error_preserves_other_calls(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    zb_h2_config_t config = config_for(2, 4096);
    zb_h2_call_id_t first = 0, second = 0, replacement = 0;
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 2));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/one"), NULL, 0, &first));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/two"), NULL, 0, &second));
    REQUIRE(exchange(connection, &peer, 1));
    nghttp2_nv invalid_response[] = {peer_nv("missing-status", "value")};
    REQUIRE(nghttp2_submit_response(peer.session, first, invalid_response, 1,
                                    NULL) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.resets, 1);
    CHECK(zb_h2_wants_input(connection));
    CHECK_OK(
        zb_h2_open(connection, sv("/s/replacement"), NULL, 0, &replacement));
    CHECK_EQ_INT(replacement, 5);
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.call_count, 3);
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_goaway_with_pending_request(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    zb_h2_config_t config = config_for(3, 4096);
    zb_h2_call_id_t first = 0, queued[2] = {0}, rejected = 77;
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 1));
    REQUIRE(exchange(connection, &peer, 1));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/one"), NULL, 0, &first));
    for (size_t i = 0; i < 2; ++i) {
        REQUIRE_OK(
            zb_h2_open(connection, sv("/s/queued"), NULL, 0, &queued[i]));
    }
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.call_count, 1);
    REQUIRE(nghttp2_submit_goaway(peer.session, NGHTTP2_FLAG_NONE, first,
                                  NGHTTP2_NO_ERROR, NULL, 0) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(zb_h2_open(connection, sv("/s/new"), NULL, 0, &rejected),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(rejected, 77);
    CHECK(zb_h2_wants_input(connection));
    REQUIRE(nghttp2_submit_rst_stream(peer.session, NGHTTP2_FLAG_NONE, first,
                                      NGHTTP2_CANCEL) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.call_count, 1);
    CHECK(!zb_h2_wants_input(connection));
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_protocol_failure_after_goaway(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    zb_h2_config_t config = config_for(1, 4096);
    zb_h2_call_id_t id = 0;
    size_t consumed = 0;
    zb_h2_output_t output = {0};
    const uint8_t invalid[] = {0,
                               0,
                               6,
                               NGHTTP2_SETTINGS,
                               0,
                               0,
                               0,
                               0,
                               0,
                               0,
                               NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE,
                               0x80,
                               0,
                               0,
                               0};
    REQUIRE_OK(zb_h2_new(&config, &connection));
    REQUIRE(peer_init(&peer, 1));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/m"), NULL, 0, &id));
    REQUIRE(exchange(connection, &peer, 1));
    REQUIRE(nghttp2_submit_goaway(peer.session, NGHTTP2_FLAG_NONE, id,
                                  NGHTTP2_NO_ERROR, NULL, 0) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    REQUIRE_OK(zb_h2_feed(connection, invalid, sizeof(invalid), &consumed));
    CHECK_EQ_INT(consumed, sizeof(invalid));
    CHECK_EQ_INT(zb_h2_next_output(connection, &output),
                 ZEROBUS_STATUS_INTERNAL);
    CHECK(!zb_h2_wants_input(connection));
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

static void test_stream_id_exhaustion_preserves_io(void)
{
    zb_h2_t *connection = NULL;
    peer_t peer = {0};
    zb_h2_config_t config = config_for(2, 4096);
    zb_h2_call_id_t first = 0, last = 0, rejected = 77;
    size_t consumed = 0;
    zb_h2_output_t output = {0};
    REQUIRE_OK(
        zb_h2_new_with_factory(&config, &connection, near_exhaustion_factory));
    REQUIRE(peer_init(&peer, 2));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/first"), NULL, 0, &first));
    REQUIRE(exchange(connection, &peer, 1));
    REQUIRE_OK(zb_h2_open(connection, sv("/s/last"), NULL, 0, &last));
    CHECK_EQ_INT(first, INT32_MAX - 2);
    CHECK_EQ_INT(last, INT32_MAX);
    CHECK_EQ_INT(zb_h2_open(connection, sv("/s/rejected"), NULL, 0, &rejected),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(rejected, 77);
    REQUIRE(exchange(connection, &peer, 1));
    REQUIRE(peer.call_count == 2);
    CHECK_EQ_INT(peer.calls[1].id, last);
    CHECK(header_is(&peer.calls[1], ":path", "/s/last"));
    nghttp2_nv response[] = {peer_nv(":status", "200"),
                             peer_nv("content-type", "application/grpc")};
    REQUIRE(nghttp2_submit_headers(peer.session, NGHTTP2_FLAG_NONE, first, NULL,
                                   response, 2, NULL) == 0);
    REQUIRE(nghttp2_submit_headers(peer.session, NGHTTP2_FLAG_NONE, last, NULL,
                                   response, 2, NULL) == 0);
    const uint8_t ping[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    REQUIRE(nghttp2_submit_ping(peer.session, NGHTTP2_FLAG_NONE, ping) == 0);
    REQUIRE(exchange(connection, &peer, 1));
    CHECK_EQ_INT(peer.resets, 0);
    CHECK_EQ_INT(peer.ping_acks, 1);
    CHECK(zb_h2_wants_input(connection));
    /* Exhausted IDs must not hide a later genuine connection error. */
    const uint8_t invalid[] = {0,
                               0,
                               6,
                               NGHTTP2_SETTINGS,
                               0,
                               0,
                               0,
                               0,
                               0,
                               0,
                               NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE,
                               0x80,
                               0,
                               0,
                               0};
    REQUIRE_OK(zb_h2_feed(connection, invalid, sizeof(invalid), &consumed));
    CHECK_EQ_INT(consumed, sizeof(invalid));
    CHECK_EQ_INT(zb_h2_next_output(connection, &output),
                 ZEROBUS_STATUS_INTERNAL);
    CHECK(!zb_h2_wants_input(connection));
zb_cleanup:
    zb_h2_free(connection);
    nghttp2_session_del(peer.session);
}

int main(void)
{
    test_configuration_validation();
    test_argument_validation();
    test_metadata_validation();
    test_settings_and_owned_request_headers();
    test_call_capacity_and_reset();
    test_peer_concurrency_queue();
    test_outbound_header_budget();
    test_output_retention_while_feeding();
    test_headers_output_retention_while_feeding();
    test_fragmented_response_header_blocks();
    test_inbound_header_budget_preserves_other_calls();
    test_malformed_input_is_sticky();
    test_invalid_settings_is_fatal();
    test_pending_ack_limit_is_fatal();
    test_stream_error_preserves_other_calls();
    test_goaway_with_pending_request();
    test_protocol_failure_after_goaway();
    test_stream_id_exhaustion_preserves_io();
    TEST_MAIN_RETURN();
}
