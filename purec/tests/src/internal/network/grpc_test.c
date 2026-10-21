/* Unit tests for network/grpc.c through its public interface. Each test acts
 * as the I/O owner: it carries bytes between the client's nghttp2 session and
 * a peer, an nghttp2 server session that plays the server. */
#include <stdlib.h>
#include <string.h>

#include "internal/network/grpc.h"
#include "test_common.h"

/* ---- fixture ----------------------------------------------------------- */

#define PATH "/databricks.zerobus.Zerobus/EphemeralStream"
#define END NGHTTP2_FLAG_END_STREAM

/* A header field for the peer to send, and an array of them with its count. */
#define NV(name, value)                                                        \
    {(uint8_t *)(name), (uint8_t *)(value), sizeof(name) - 1,                  \
     sizeof(value) - 1, NGHTTP2_NV_FLAG_NONE}
#define FIELDS(array) (array), (sizeof(array) / sizeof((array)[0]))

/* Call i opens stream 2i + 1. */
enum { CALL_STREAM = 1, OTHER_STREAM = 3, CALLS = 3, MAX_QUEUED = 4 };

static const nghttp2_nv grpc_headers[] = {
    NV(":status", "200"), NV("content-type", "application/grpc")};
static const nghttp2_nv ok_trailers[] = {NV("grpc-status", "0")};
/* A trailers-only response: one block with the headers and the status. */
static const nghttp2_nv ok_only[] = {NV(":status", "200"),
                                     NV("content-type", "application/grpc"),
                                     NV("grpc-status", "0")};
/* gRPC messages "xx" and "y". */
static const uint8_t xx_y[] = {0, 0, 0, 0, 2, 'x', 'x', 0, 0, 0, 0, 1, 'y'};

/* One call's callbacks: messages for pull_send to hand out, and what
 * on_message and on_complete received. */
typedef struct {
    zb_message_t *queue[MAX_QUEUED];
    int queued;
    int pulled;
    bool eof; /* pull_send reports EOF once the queue is drained */
    unsigned refuse_offers; /* bit n set: on_message refuses its nth offer */
    int offers;
    int delivered;
    char received[64]; /* delivered payloads, concatenated */
    int completions;
    zb_rpc_result_t result; /* result.message is copied into message */
    char message[64];
    zb_grpc_call_t *owned; /* a heap call to use, which on_complete frees */
} test_context_t;

/* What the client sent on one stream, as the peer received it. */
typedef struct {
    char headers[512]; /* "name: value\n" per field */
    uint8_t data[64];  /* the start of the DATA payload */
    size_t data_len;   /* all of it */
    int data_frames;
    bool ended;     /* the client ended the stream */
    uint32_t reset; /* the client's RST_STREAM code, or UINT32_MAX */
} sent_t;

/* The other end of the connection: an nghttp2 server session, what it
 * received, and the bytes queued for the client, in the order sent. */
typedef struct {
    nghttp2_session *session;
    sent_t sent[CALLS]; /* sent[i] is call i's stream */
    uint32_t goaway;    /* the client's GOAWAY code, or UINT32_MAX */
    uint8_t out[16384];
    size_t out_len;
} peer_t;

/* The state of the running test, reset by start. */
static zb_grpc_t grpc;
static zb_grpc_call_t calls[CALLS];
static test_context_t ctx[CALLS];
static peer_t peer;

static zb_message_t *pull_send(void *context, bool *eof)
{
    test_context_t *c = context;
    if (c->pulled < c->queued) {
        return c->queue[c->pulled++];
    }
    *eof = c->eof;
    return NULL;
}

static bool on_message(void *context, zb_message_t *message)
{
    test_context_t *c = context;
    if (c->refuse_offers & (1u << c->offers++)) {
        return false;
    }
    size_t used = strlen(c->received);
    if (message->len != 0 && message->len < sizeof(c->received) - used) {
        memcpy(c->received + used, message->data, message->len);
    }
    c->delivered++;
    free(message->data);
    free(message);
    return true;
}

static void on_complete(void *context, const zb_rpc_result_t *result)
{
    test_context_t *c = context;
    c->completions++;
    c->result = *result;
    c->result.message = NULL;
    snprintf(c->message, sizeof(c->message), "%s",
             result->message != NULL ? result->message : "");
    free(result->message);
    free(c->owned);
    c->owned = NULL;
}

/* Queues a message of len bytes, each set to fill, for call i. */
static void queue_message(int i, size_t len, uint8_t fill)
{
    zb_message_t *message = calloc(1, sizeof(*message));
    if (message != NULL && len != 0 && (message->data = malloc(len)) != NULL) {
        memset(message->data, fill, len);
        message->len = len;
    }
    ctx[i].queue[ctx[i].queued++] = message;
}

static zb_grpc_config_t test_config(void)
{
    zb_grpc_config_t config = {.authority = sv("localhost:8080"),
                               .tls = true,
                               .max_header_bytes = 4096,
                               .max_message_bytes = 1024};
    return config;
}

/* Opens call i (or the heap call in ctx[i].owned) with the test callbacks
 * and the given request. Nothing is sent yet. */
static zerobus_status_t open_request(int i, const char *path,
                                     const zb_metadata_t *headers,
                                     size_t header_count)
{
    zb_grpc_call_config_t config = {.path = sv(path),
                                    .headers = headers,
                                    .header_count = header_count,
                                    .context = &ctx[i],
                                    .pull_send = pull_send,
                                    .on_message = on_message,
                                    .on_complete = on_complete};
    zb_grpc_call_t *call = ctx[i].owned != NULL ? ctx[i].owned : &calls[i];
    return zb_grpc_call_open(&grpc, call, &config);
}

/* Opens call i with the test path and metadata. Nothing is sent yet. */
static zerobus_status_t add_call(int i)
{
    const zb_metadata_t headers[] = {
        {sv("authorization"), sv("Bearer token")},
        {sv("x-databricks-zerobus-table-name"), sv("c.s.t")}};
    return open_request(i, PATH, headers, 2);
}

/* ---- the peer ---------------------------------------------------------- */

static sent_t *sent_on(int32_t stream_id)
{
    return stream_id > 0 && stream_id % 2 == 1 && stream_id / 2 < CALLS
               ? &peer.sent[stream_id / 2]
               : NULL;
}

static int peer_on_header(nghttp2_session *session, const nghttp2_frame *frame,
                          const uint8_t *name, size_t name_len,
                          const uint8_t *value, size_t value_len, uint8_t flags,
                          void *user_data)
{
    (void)session;
    (void)user_data;
    sent_t *sent = sent_on(frame->hd.stream_id);
    if (sent != NULL) {
        size_t used = strlen(sent->headers);
        snprintf(sent->headers + used, sizeof(sent->headers) - used,
                 "%.*s: %.*s%s\n", (int)name_len, (const char *)name,
                 (int)value_len, (const char *)value,
                 (flags & NGHTTP2_NV_FLAG_NO_INDEX) ? " (never indexed)" : "");
    }
    return 0;
}

static int peer_on_data(nghttp2_session *session, uint8_t flags,
                        int32_t stream_id, const uint8_t *data, size_t len,
                        void *user_data)
{
    (void)session;
    (void)flags;
    (void)user_data;
    sent_t *sent = sent_on(stream_id);
    if (sent != NULL) {
        if (sent->data_len < sizeof(sent->data)) {
            size_t room = sizeof(sent->data) - sent->data_len;
            memcpy(sent->data + sent->data_len, data, len < room ? len : room);
        }
        sent->data_len += len;
    }
    return 0;
}

static int peer_on_frame(nghttp2_session *session, const nghttp2_frame *frame,
                         void *user_data)
{
    (void)session;
    (void)user_data;
    if (frame->hd.type == NGHTTP2_GOAWAY) {
        peer.goaway = frame->goaway.error_code;
        return 0;
    }
    sent_t *sent = sent_on(frame->hd.stream_id);
    if (sent == NULL) {
        return 0;
    }
    if (frame->hd.type == NGHTTP2_RST_STREAM) {
        sent->reset = frame->rst_stream.error_code;
    } else if (frame->hd.type == NGHTTP2_DATA ||
               frame->hd.type == NGHTTP2_HEADERS) {
        sent->data_frames += frame->hd.type == NGHTTP2_DATA;
        sent->ended |= (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0;
    }
    return 0;
}

static void peer_put(const void *data, size_t len)
{
    bool fits = len <= sizeof(peer.out) - peer.out_len;
    CHECK(fits);
    if (fits && len != 0) {
        memcpy(peer.out + peer.out_len, data, len);
        peer.out_len += len;
    }
}

/* Moves what the peer session has queued into out. */
static void peer_flush(void)
{
    const uint8_t *data = NULL;
    ssize_t n;
    while ((n = nghttp2_session_mem_send(peer.session, &data)) > 0) {
        peer_put(data, (size_t)n);
    }
    CHECK_EQ_INT(n, 0);
}

static void peer_init(void)
{
    nghttp2_session_callbacks *callbacks = NULL;
    for (int i = 0; i < CALLS; ++i) {
        peer.sent[i].reset = UINT32_MAX;
    }
    peer.goaway = UINT32_MAX;
    if (nghttp2_session_callbacks_new(&callbacks) == 0) {
        nghttp2_session_callbacks_set_on_header_callback(callbacks,
                                                         peer_on_header);
        nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks,
                                                                  peer_on_data);
        nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks,
                                                             peer_on_frame);
        if (nghttp2_session_server_new(&peer.session, callbacks, NULL) == 0) {
            (void)nghttp2_submit_settings(peer.session, NGHTTP2_FLAG_NONE, NULL,
                                          0);
        }
        nghttp2_session_callbacks_del(callbacks);
    }
    CHECK(peer.session != NULL);
}

/* The peer's frames, queued for the client in the order of these calls. */
static void peer_headers(int32_t stream_id, uint8_t flags,
                         const nghttp2_nv *fields, size_t count)
{
    CHECK(nghttp2_submit_headers(peer.session, flags, stream_id, NULL, fields,
                                 count, NULL) >= 0);
    peer_flush();
}

/* Written by hand, since a DATA frame needs no HPACK state. The tests' stream
 * ids and payload lengths fit a byte. */
static void peer_data(uint8_t stream_id, uint8_t flags, const void *data,
                      uint8_t len)
{
    const uint8_t header[9] = {0, 0, len, NGHTTP2_DATA, flags,
                               0, 0, 0,   stream_id};
    peer_put(header, sizeof(header));
    peer_put(data, len);
}

static void peer_reset(int32_t stream_id, uint32_t error)
{
    CHECK_EQ_INT(nghttp2_submit_rst_stream(peer.session, NGHTTP2_FLAG_NONE,
                                           stream_id, error),
                 0);
    peer_flush();
}

static void peer_goaway(int32_t last_stream_id, uint32_t error)
{
    CHECK_EQ_INT(nghttp2_submit_goaway(peer.session, NGHTTP2_FLAG_NONE,
                                       last_stream_id, error, NULL, 0),
                 0);
    peer_flush();
}

/* ---- the owner's side -------------------------------------------------- */

/* Carries bytes between the client and the peer, as the I/O owner would,
 * until neither has more to send. Returns 0 or the client's nghttp2 error.
 * The client must take all of its input; tests that pause use receive. */
static int pump(void)
{
    for (;;) {
        bool idle = peer.out_len == 0;
        if (!idle) {
            ssize_t got =
                nghttp2_session_mem_recv(grpc.session, peer.out, peer.out_len);
            if (got < 0) {
                return (int)got;
            }
            CHECK_EQ_INT(got, peer.out_len);
            peer.out_len = 0;
        }
        const uint8_t *data = NULL;
        ssize_t n;
        while ((n = nghttp2_session_mem_send(grpc.session, &data)) > 0) {
            CHECK_EQ_INT(
                nghttp2_session_mem_recv(peer.session, data, (size_t)n), n);
            idle = false;
        }
        if (n < 0) {
            return (int)n;
        }
        peer_flush();
        if (idle && peer.out_len == 0) {
            return 0;
        }
    }
}

/* Passes the peer's output from byte at to the client in one mem_recv, as an
 * owner with one input buffer would, and returns mem_recv's result. The
 * output is emptied once the client has taken all of it and is not paused
 * (paused, it still needs the rest, even if empty, after the retry). */
static ssize_t receive(size_t at)
{
    ssize_t got = nghttp2_session_mem_recv(grpc.session, peer.out + at,
                                           peer.out_len - at);
    if (got >= 0 && !grpc.paused && at + (size_t)got == peer.out_len) {
        peer.out_len = 0;
    }
    return got;
}

/* Frees the running test's state. */
static void stop(void)
{
    zb_grpc_destroy(&grpc);
    nghttp2_session_del(peer.session);
    for (int i = 0; i < CALLS; ++i) {
        for (int m = ctx[i].pulled; m < ctx[i].queued; ++m) {
            if (ctx[i].queue[m] != NULL) {
                free(ctx[i].queue[m]->data);
                free(ctx[i].queue[m]);
            }
        }
        free(ctx[i].owned);
    }
    memset(calls, 0, sizeof(calls));
    memset(ctx, 0, sizeof(ctx));
    memset(&peer, 0, sizeof(peer));
}

/* Starts a test with a fresh session and peer, and no calls. */
static void start(void)
{
    zb_grpc_config_t config = test_config();
    stop();
    peer_init();
    CHECK_OK(zb_grpc_init(&grpc, &config));
}

/* Opens call i and carries its request HEADERS to the peer. */
static zerobus_status_t open_call(int i)
{
    zerobus_status_t status = add_call(i);
    if (status == ZEROBUS_STATUS_OK && pump() != 0) {
        status = ZEROBUS_STATUS_INTERNAL;
    }
    return status;
}

/* Starts a test with one call that the peer answers with one header block.
 * Returns the call's status, or UINT32_MAX while the call is still open. */
static uint32_t respond(uint8_t flags, const nghttp2_nv *fields, size_t count)
{
    start();
    if (open_call(0) == ZEROBUS_STATUS_OK) {
        peer_headers(CALL_STREAM, flags, fields, count);
        (void)pump();
    }
    return ctx[0].completions == 1 ? (uint32_t)ctx[0].result.status
                                   : UINT32_MAX;
}

/* ---- tests ------------------------------------------------------------- */

static void test_init_validation(void)
{
    zb_grpc_config_t config = test_config();
    zb_grpc_config_t bad = config;
    const zb_grpc_call_config_t no_pull = {
        .path = sv(PATH), .context = &ctx[0], .on_message = on_message};
    const zb_metadata_t upper = {sv("Upper"), sv("v")};
    char big[301] = {0};
    memset(big, 'v', sizeof(big) - 1);
    const zb_metadata_t large = {sv("x-big"), sv(big)};
    stop();

    CHECK_EQ_INT(zb_grpc_init(NULL, &config), ZEROBUS_STATUS_INVALID_ARGUMENT);
    bad.max_message_bytes = 0;
    CHECK_EQ_INT(zb_grpc_init(&grpc, &bad), ZEROBUS_STATUS_INVALID_ARGUMENT);
    bad = config;
    bad.max_header_bytes = 0;
    CHECK_EQ_INT(zb_grpc_init(&grpc, &bad), ZEROBUS_STATUS_INVALID_ARGUMENT);
    bad = config;
    bad.authority = sv("user@host");
    CHECK_EQ_INT(zb_grpc_init(&grpc, &bad), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(grpc.session == NULL);

    /* A call needs an initialized session, callbacks and a valid request. A
     * failed open leaves the call zeroed. */
    CHECK_EQ_INT(open_request(0, PATH, NULL, 0),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    REQUIRE_OK(zb_grpc_init(&grpc, &config));
    CHECK_EQ_INT(zb_grpc_call_open(&grpc, &calls[0], &no_pull),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(open_request(0, PATH, NULL, 1),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(open_request(0, "no-slash", NULL, 0),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(open_request(0, PATH, &upper, 1),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(open_request(0, PATH, &large, SIZE_MAX),
                 ZEROBUS_STATUS_OUT_OF_RANGE);
    CHECK(calls[0].grpc == NULL);

    /* An open call cannot be opened again, and a failed session refuses new
     * calls with its failure. */
    REQUIRE_OK(open_request(0, PATH, NULL, 0));
    CHECK_EQ_INT(open_request(0, PATH, NULL, 0),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zb_grpc_abort(&grpc, ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(open_request(1, PATH, NULL, 0), ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(ctx[0].completions, 1);

    /* The request headers must fit max_header_bytes. */
    zb_grpc_destroy(&grpc);
    config.max_header_bytes = 512;
    REQUIRE_OK(zb_grpc_init(&grpc, &config));
    CHECK_EQ_INT(open_request(0, PATH, &large, 1), ZEROBUS_STATUS_OUT_OF_RANGE);
zb_cleanup:
    stop();
}

static void test_request(void)
{
    const char *expected_headers =
        ":method: POST\n"
        ":scheme: https\n"
        ":authority: localhost:8080\n"
        ":path: " PATH "\n"
        "content-type: application/grpc\n"
        "te: trailers\n"
        "authorization: Bearer token (never indexed)\n"
        "x-databricks-zerobus-table-name: c.s.t\n";
    const uint8_t framed[] = {0, 0, 0, 0, 3, 'a', 'a', 'a', 0, 0, 0, 0, 0};
    const uint8_t large_start[] = {0, 0, 0, 0x4e, 0x20, 'z'}; /* 20000 */
    const sent_t *sent = &peer.sent[0];

    /* The first send carries our SETTINGS and the request HEADERS, with the
     * credential never indexed. With nothing queued there is no DATA. */
    start();
    REQUIRE_OK(open_call(0));
    CHECK_EQ_INT(nghttp2_session_get_remote_settings(
                     peer.session, NGHTTP2_SETTINGS_ENABLE_PUSH),
                 0);
    CHECK_EQ_INT(nghttp2_session_get_remote_settings(
                     peer.session, NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE),
                 4096);
    CHECK(strcmp(sent->headers, expected_headers) == 0);
    CHECK_EQ_INT(sent->data_frames, 0);

    /* Queued messages are framed into DATA once the owner resumes the call. A
     * message larger than a frame spans two, and END_STREAM rides on the frame
     * that finishes it. */
    queue_message(0, 3, 'a');
    queue_message(0, 0, 0);
    CHECK_EQ_INT(nghttp2_session_resume_data(grpc.session, CALL_STREAM), 0);
    CHECK_EQ_INT(pump(), 0);
    CHECK(sent->data_len == sizeof(framed) &&
          memcmp(sent->data, framed, sizeof(framed)) == 0);
    CHECK(!sent->ended);
    queue_message(0, 20000, 'z');
    ctx[0].eof = true;
    CHECK_EQ_INT(nghttp2_session_resume_data(grpc.session, CALL_STREAM), 0);
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(sent->data_frames, 3);
    CHECK_EQ_INT(sent->data_len, sizeof(framed) + 5 + 20000);
    CHECK(memcmp(sent->data + sizeof(framed), large_start,
                 sizeof(large_start)) == 0);
    CHECK(sent->ended && calls[0].request_closed);

#if SIZE_MAX > UINT32_MAX
    /* A message too long for the 4-byte length prefix fails the call before
     * any of it is sent. Its data is never read, so a short buffer stands in
     * for it. */
    start();
    REQUIRE_OK(open_call(0));
    queue_message(0, 1, 'q');
    REQUIRE(ctx[0].queue[0] != NULL);
    ctx[0].queue[0]->len = (size_t)UINT32_MAX + 1;
    CHECK_EQ_INT(nghttp2_session_resume_data(grpc.session, CALL_STREAM), 0);
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_OUT_OF_RANGE);
    CHECK_EQ_INT(sent->data_frames, 0);
    CHECK_EQ_INT(sent->reset, NGHTTP2_INTERNAL_ERROR);
#endif
zb_cleanup:
    stop();
}

static void test_response(void)
{
    const nghttp2_nv informational[] = {NV(":status", "100")};
    const nghttp2_nv trailers[] = {NV("grpc-status", "0"),
                                   NV("grpc-message", "all%20good")};

    /* A 1xx block, the response headers, two messages in one DATA frame, then
     * OK trailers with a percent-encoded grpc-message. Both sides end the
     * stream, so nothing is reset. */
    start();
    ctx[0].eof = true; /* half-closes at open */
    REQUIRE_OK(open_call(0));
    peer_headers(CALL_STREAM, 0, FIELDS(informational));
    peer_headers(CALL_STREAM, 0, FIELDS(grpc_headers));
    peer_data(CALL_STREAM, 0, xx_y, sizeof(xx_y));
    peer_headers(CALL_STREAM, END, FIELDS(trailers));
    CHECK_EQ_INT(pump(), 0);
    CHECK(strcmp(ctx[0].received, "xxy") == 0);
    CHECK_EQ_INT(ctx[0].completions, 1);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_OK);
    CHECK(ctx[0].result.has_grpc_status);
    CHECK_EQ_INT(ctx[0].result.http_status, 200);
    CHECK(strcmp(ctx[0].message, "all good") == 0);
    CHECK_EQ_INT(peer.sent[0].reset, UINT32_MAX);
zb_cleanup:
    stop();
}

static void test_response_paused(void)
{
    const uint8_t xyz[] = {0, 0, 0,   0, 2, 'x', 'x', 0, 0,  0,
                           0, 1, 'y', 0, 0, 0,   0,   1, 'z'};
    /* 'x', then the prefix of a message over max_message_bytes (2000). */
    const uint8_t then_too_long[] = {0, 0, 0, 0, 1, 'x', 0, 0, 0, 0x07, 0xd0};
    ssize_t n = 0;

    /* A refused message pauses input: mem_recv stops early. Each retry offers
     * it again and decodes the rest of its chunk, until another refusal ("y")
     * pauses again. Then the owner passes on the rest of its input. */
    start();
    REQUIRE_OK(open_call(0));
    ctx[0].refuse_offers = 0x5; /* offers 0 and 2 */
    peer_headers(CALL_STREAM, 0, FIELDS(grpc_headers));
    peer_data(CALL_STREAM, 0, xyz, sizeof(xyz));
    peer_headers(CALL_STREAM, END, FIELDS(ok_trailers));
    n = receive(0);
    REQUIRE(n >= 0 && grpc.paused);
    CHECK_EQ_INT(ctx[0].delivered, 0);
    CHECK_OK(zb_grpc_retry_receive(&grpc));
    CHECK(grpc.paused);
    CHECK(strcmp(ctx[0].received, "xx") == 0);
    CHECK_OK(zb_grpc_retry_receive(&grpc));
    CHECK(!grpc.paused);
    CHECK(strcmp(ctx[0].received, "xxyz") == 0);
    CHECK(receive((size_t)n) >= 0 && peer.out_len == 0);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_OK);

    /* A decoding error in the rest of the chunk during the retry fails only
     * the call and cancels its stream. */
    start();
    REQUIRE_OK(open_call(0));
    ctx[0].refuse_offers = 1;
    peer_headers(CALL_STREAM, 0, FIELDS(grpc_headers));
    peer_data(CALL_STREAM, 0, then_too_long, sizeof(then_too_long));
    n = receive(0);
    REQUIRE(n >= 0 && grpc.paused);
    CHECK_OK(zb_grpc_retry_receive(&grpc));
    CHECK_EQ_INT(ctx[0].delivered, 1);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_OUT_OF_RANGE);
    CHECK(receive((size_t)n) >= 0);
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(peer.sent[0].reset, NGHTTP2_CANCEL);
zb_cleanup:
    stop();
}

static void test_response_status(void)
{
    const nghttp2_nv unauthenticated[] = {
        NV(":status", "200"), NV("content-type", "application/grpc"),
        NV("grpc-status", "16")};
    const nghttp2_nv unavailable[] = {NV(":status", "503"),
                                      NV("content-type", "text/html")};

    /* grpc-status maps its gRPC code, a non-gRPC HTTP error maps its HTTP
     * status, and a peer reset maps its HTTP/2 error code. A response that
     * ends while the request is still open resets the stream with NO_ERROR. */
    CHECK_EQ_INT(respond(END, FIELDS(unauthenticated)),
                 ZEROBUS_STATUS_UNAUTHENTICATED);
    CHECK(ctx[0].result.has_grpc_status);
    CHECK_EQ_INT(peer.sent[0].reset, NGHTTP2_NO_ERROR);
    CHECK_EQ_INT(respond(0, FIELDS(unavailable)), ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(ctx[0].result.http_status, 503);
    start();
    REQUIRE_OK(open_call(0));
    peer_reset(CALL_STREAM, NGHTTP2_CANCEL);
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_CANCELLED);
    CHECK_EQ_INT(ctx[0].result.http2_error, NGHTTP2_CANCEL);
zb_cleanup:
    stop();
}

static void test_response_checks(void)
{
    static char big[4100];
    const nghttp2_nv bad_status[] = {NV(":status", "200"),
                                     NV("content-type", "application/grpc"),
                                     NV("grpc-status", "ok")};
    const nghttp2_nv not_grpc[] = {NV(":status", "200"),
                                   NV("content-type", "application/grpcx")};
    memset(big, 'v', sizeof(big));
    const nghttp2_nv oversized[] = {NV(":status", "200"),
                                    {(uint8_t *)"x-big", (uint8_t *)big, 5,
                                     sizeof(big), NGHTTP2_NV_FLAG_NONE}};
    const uint8_t partial[] = {0, 0, 0, 0, 4, 'x'};
    const uint8_t compressed[] = {1, 0, 0, 0, 1, 'x'};

    /* grpc-status must be a decimal, and only in the block that ends the
     * stream. The content type must be gRPC, and a header block must fit
     * max_header_bytes (4096). An invalid header resets the stream with
     * PROTOCOL_ERROR. */
    CHECK_EQ_INT(respond(END, FIELDS(bad_status)), ZEROBUS_STATUS_INTERNAL);
    CHECK_EQ_INT(respond(0, FIELDS(ok_only)), ZEROBUS_STATUS_INTERNAL);
    CHECK_EQ_INT(respond(0, FIELDS(not_grpc)), ZEROBUS_STATUS_INTERNAL);
    CHECK_EQ_INT(respond(0, FIELDS(oversized)), ZEROBUS_STATUS_OUT_OF_RANGE);
    CHECK_EQ_INT(peer.sent[0].reset, NGHTTP2_PROTOCOL_ERROR);
    CHECK_EQ_INT(peer.sent[0].reset, ctx[0].result.http2_error);

    /* After the response headers, the stream may not end inside a message,
     * and HTTP 200 alone (no trailers) is not OK. */
    CHECK_EQ_INT(respond(0, FIELDS(grpc_headers)), UINT32_MAX);
    peer_data(CALL_STREAM, END, partial, sizeof(partial));
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_INTERNAL);
    CHECK_EQ_INT(respond(0, FIELDS(grpc_headers)), UINT32_MAX);
    peer_data(CALL_STREAM, END, xx_y, 7); /* just "xx" */
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(ctx[0].delivered, 1);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_UNKNOWN);

    /* A message the decoder rejects fails the call and cancels its stream. */
    CHECK_EQ_INT(respond(0, FIELDS(grpc_headers)), UINT32_MAX);
    peer_data(CALL_STREAM, 0, compressed, sizeof(compressed));
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_UNIMPLEMENTED);
    CHECK_EQ_INT(peer.sent[0].reset, NGHTTP2_CANCEL);
    stop();
}

static void test_goaway(void)
{
    /* A graceful GOAWAY fails the calls above its last_stream_id with its
     * error code, and lets the ones it covers finish. */
    start();
    REQUIRE_OK(open_call(0));
    REQUIRE_OK(open_call(1));
    peer_goaway(CALL_STREAM, NGHTTP2_NO_ERROR);
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(ctx[1].result.status, ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(ctx[1].result.http2_error, NGHTTP2_NO_ERROR);
    CHECK_EQ_INT(ctx[0].completions, 0);
    peer_headers(CALL_STREAM, END, FIELDS(ok_only));
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_OK);

    /* An error GOAWAY fails even the calls it covers. The result keeps the
     * peer's code; the stream is reset with CANCEL. */
    start();
    REQUIRE_OK(open_call(0));
    peer_goaway(CALL_STREAM, NGHTTP2_ENHANCE_YOUR_CALM);
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(ctx[0].result.http2_error, NGHTTP2_ENHANCE_YOUR_CALM);
    CHECK_EQ_INT(peer.sent[0].reset, NGHTTP2_CANCEL);

    /* After any GOAWAY, nghttp2 opens no new streams: a request still queued,
     * even under a GOAWAY that covers it, is never sent and fails. No HTTP/2
     * error is reported, since the stream never existed. */
    start();
    REQUIRE_OK(add_call(0));
    peer_goaway(0x7fffffff, NGHTTP2_NO_ERROR);
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(ctx[0].result.http2_error, 0);
    CHECK(peer.sent[0].headers[0] == '\0');

    /* A connection error (DATA on stream 0) makes nghttp2 send an error
     * GOAWAY. It reaches the peer, the session fails, and nghttp2 wants no
     * more I/O. */
    start();
    REQUIRE_OK(open_call(0));
    peer_data(0, 0, "x", 1);
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(peer.goaway, NGHTTP2_PROTOCOL_ERROR);
    CHECK_EQ_INT(grpc.failure, ZEROBUS_STATUS_INTERNAL);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_INTERNAL);
    CHECK(!nghttp2_session_want_read(grpc.session));
    CHECK(!nghttp2_session_want_write(grpc.session));
zb_cleanup:
    stop();
}

static void test_abort_and_destroy(void)
{
    /* Retrying does nothing while not paused and rejects NULL. Abort completes
     * every call; OK becomes UNAVAILABLE, and only the first abort counts. */
    start();
    REQUIRE_OK(add_call(0));
    REQUIRE_OK(add_call(1));
    CHECK_OK(zb_grpc_retry_receive(&grpc));
    CHECK_EQ_INT(zb_grpc_retry_receive(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    zb_grpc_abort(&grpc, ZEROBUS_STATUS_OK);
    zb_grpc_abort(&grpc, ZEROBUS_STATUS_INTERNAL);
    zb_grpc_abort(NULL, ZEROBUS_STATUS_INTERNAL);
    CHECK_EQ_INT(grpc.failure, ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(ctx[1].result.status, ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zb_grpc_retry_receive(&grpc), ZEROBUS_STATUS_UNAVAILABLE);
    zb_grpc_destroy(&grpc);
    CHECK(ctx[0].completions == 1 && ctx[1].completions == 1);

    /* Destroy completes unfinished calls as CANCELLED, once, and accepts
     * destroyed and NULL state. Cancel does nothing for a completed call. */
    start();
    REQUIRE_OK(add_call(0));
    zb_grpc_destroy(&grpc);
    zb_grpc_destroy(&grpc);
    zb_grpc_destroy(NULL);
    zb_grpc_call_cancel(&calls[0]);
    zb_grpc_call_cancel(NULL);
    CHECK_EQ_INT(ctx[0].completions, 1);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_CANCELLED);
    CHECK(grpc.session == NULL);
zb_cleanup:
    stop();
}

static void test_multiple_calls(void)
{
    const nghttp2_nv unauthenticated[] = {NV("grpc-status", "16")};
    const uint8_t x[] = {0, 0, 0, 0, 1, 'x'};
    const uint8_t y[] = {0, 0, 0, 0, 1, 'y'};
    const uint8_t compressed[] = {1, 0, 0, 0, 1, 'z'};

    /* Two calls on one session: each one's messages go out on its own
     * stream, and each gets its own responses, even interleaved. */
    start();
    REQUIRE_OK(open_call(0));
    REQUIRE_OK(open_call(1));
    queue_message(0, 1, 'a');
    queue_message(1, 1, 'b');
    CHECK_EQ_INT(nghttp2_session_resume_data(grpc.session, CALL_STREAM), 0);
    CHECK_EQ_INT(nghttp2_session_resume_data(grpc.session, OTHER_STREAM), 0);
    CHECK_EQ_INT(pump(), 0);
    CHECK(peer.sent[0].data_len == 6 && peer.sent[0].data[5] == 'a');
    CHECK(peer.sent[1].data_len == 6 && peer.sent[1].data[5] == 'b');
    peer_headers(CALL_STREAM, 0, FIELDS(grpc_headers));
    peer_headers(OTHER_STREAM, 0, FIELDS(grpc_headers));
    peer_data(OTHER_STREAM, 0, y, sizeof(y));
    peer_data(CALL_STREAM, 0, x, sizeof(x));
    peer_headers(OTHER_STREAM, END, FIELDS(unauthenticated));
    CHECK_EQ_INT(pump(), 0);
    CHECK(strcmp(ctx[0].received, "x") == 0);
    CHECK(strcmp(ctx[1].received, "y") == 0);
    CHECK_EQ_INT(ctx[1].result.status, ZEROBUS_STATUS_UNAUTHENTICATED);
    CHECK_EQ_INT(ctx[0].completions, 0);

    /* A failure or a cancel ends only its own call and resets only its
     * stream, and the session carries on. The calls left open stay tracked:
     * here a middle and then the oldest call end, and abort still reaches the
     * newest. */
    start();
    REQUIRE_OK(open_call(0));
    REQUIRE_OK(open_call(1));
    REQUIRE_OK(open_call(2));
    peer_headers(OTHER_STREAM, 0, FIELDS(grpc_headers));
    peer_data(OTHER_STREAM, 0, compressed, sizeof(compressed));
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(ctx[1].result.status, ZEROBUS_STATUS_UNIMPLEMENTED);
    CHECK_EQ_INT(ctx[0].completions, 0);
    zb_grpc_call_cancel(&calls[0]);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_CANCELLED);
    CHECK_EQ_INT(ctx[0].result.http2_error, NGHTTP2_CANCEL);
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(peer.sent[0].reset, NGHTTP2_CANCEL);
    CHECK_EQ_INT(peer.sent[1].reset, NGHTTP2_CANCEL);
    CHECK_EQ_INT(grpc.failure, ZEROBUS_STATUS_OK);
    zb_grpc_abort(&grpc, ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(ctx[2].completions, 1);
    CHECK_EQ_INT(ctx[2].result.status, ZEROBUS_STATUS_UNAVAILABLE);
zb_cleanup:
    stop();
}

static void test_calls_freed_on_complete(void)
{
    /* Calls may be freed from on_complete: a GOAWAY and a cancel end heap
     * calls, and each one frees itself. */
    start();
    ctx[0].owned = calloc(1, sizeof(zb_grpc_call_t));
    ctx[1].owned = calloc(1, sizeof(zb_grpc_call_t));
    REQUIRE(ctx[0].owned != NULL && ctx[1].owned != NULL);
    REQUIRE_OK(open_call(0));
    REQUIRE_OK(open_call(1));
    peer_goaway(0, NGHTTP2_NO_ERROR);
    CHECK_EQ_INT(pump(), 0);
    CHECK(ctx[0].owned == NULL && ctx[1].owned == NULL);
    CHECK_EQ_INT(ctx[0].result.status, ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(ctx[1].result.status, ZEROBUS_STATUS_UNAVAILABLE);
    start();
    ctx[0].owned = calloc(1, sizeof(zb_grpc_call_t));
    REQUIRE(ctx[0].owned != NULL);
    REQUIRE_OK(open_call(0));
    zb_grpc_call_cancel(ctx[0].owned);
    CHECK(ctx[0].owned == NULL);
    CHECK_EQ_INT(pump(), 0);
    CHECK_EQ_INT(peer.sent[0].reset, NGHTTP2_CANCEL);
zb_cleanup:
    stop();
}

static void test_multiple_calls_pause(void)
{
    const uint8_t xw[] = {0, 0, 0, 0, 1, 'x', 0, 0, 0, 0, 1, 'w'};
    const uint8_t y[] = {0, 0, 0, 0, 1, 'y'};
    ssize_t n = 0;

    /* A refused message on one call pauses input for every call. Cancelling
     * the paused call drops the rest of its chunk ("w"), and input continues
     * for the other call. */
    start();
    REQUIRE_OK(open_call(0));
    REQUIRE_OK(open_call(1));
    ctx[0].refuse_offers = 1;
    peer_headers(CALL_STREAM, 0, FIELDS(grpc_headers));
    peer_headers(OTHER_STREAM, 0, FIELDS(grpc_headers));
    peer_data(CALL_STREAM, 0, xw, sizeof(xw));
    peer_data(OTHER_STREAM, 0, y, sizeof(y));
    n = receive(0);
    REQUIRE(n >= 0 && grpc.paused);
    CHECK_EQ_INT(ctx[1].delivered, 0);
    zb_grpc_call_cancel(&calls[0]);
    CHECK_OK(zb_grpc_retry_receive(&grpc));
    CHECK(!grpc.paused);
    CHECK_EQ_INT(ctx[0].delivered, 0);
    CHECK(receive((size_t)n) >= 0 && peer.out_len == 0);
    CHECK(strcmp(ctx[1].received, "y") == 0);
zb_cleanup:
    stop();
}

int main(void)
{
    test_init_validation();
    test_request();
    test_response();
    test_response_paused();
    test_response_status();
    test_response_checks();
    test_goaway();
    test_abort_and_destroy();
    test_multiple_calls();
    test_calls_freed_on_complete();
    test_multiple_calls_pause();
    TEST_MAIN_RETURN();
}
