/* Unit tests for network/transport.c, and through it io.c, against a gRPC
 * server on a loopback port: an nghttp2 server session on a thread of its
 * own, over h2c or TLS. */
#define _GNU_SOURCE

#include <nghttp2/nghttp2.h>
#include <openssl/ssl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "internal/network/transport.h"
#include "loopback.h"
#include "test_common.h"

#define CERT_FILE ZB_TEST_DIR "/cert.pem"
#define KEY_FILE ZB_TEST_DIR "/key.pem"

/* What the server does with a call depends on its path. */
#define ECHO "/test.Test/Echo"      /* returns each message, OK once ended */
#define SINK "/test.Test/Sink"      /* drops the messages, OK once ended */
#define HANG_UP "/test.Test/HangUp" /* closes the connection */
#define BREAK "/test.Test/Break"    /* sends DATA on stream 0 */
#define GO_AWAY "/test.Test/GoAway" /* sends GOAWAY, then OK at once */

enum {
    TIMEOUT_MS = 5000,
    SHORT_TIMEOUT_MS = 20,
    STREAMS = 32,
    /* Larger than the I/O buffer and HTTP/2's initial flow-control window. */
    BIG_MESSAGE = 256 * 1024,
    /* An upload of big messages, more than the client's socket buffer holds,
     * to a server with flow-control windows so large that only the socket
     * holds the client back. */
    UPLOAD_MESSAGES = 32,
    LARGE_WINDOW = 64 * 1024 * 1024,
    /* A stalling server's receive buffer, and how long it stops reading. */
    STALL_BUFFER = 16 * 1024,
    STALL_MS = 100,
    /* A pinging server's PING goes out after PING_AFTER request bytes, and
     * its ACK must come back within PING_SLACK more. That leaves room for
     * data still in the sockets, but is well short of the rest of the upload,
     * after which a client that never reads would answer. */
    PING_AFTER = 1024 * 1024,
    PING_SLACK = 4 * 1024 * 1024,
    WORKERS = 4,
    CALLS_PER_WORKER = 4,
};

#define NV(name, value)                                                        \
    {(uint8_t *)(name), (uint8_t *)(value), sizeof(name) - 1,                  \
     sizeof(value) - 1, NGHTTP2_NV_FLAG_NONE}

static const nghttp2_nv response[] = {NV(":status", "200"),
                                      NV("content-type", "application/grpc")};
static const nghttp2_nv ok_trailers[] = {NV("grpc-status", "0")};

static zb_tls_ctx_t *trusted; /* Trusts the test certificate. */

/* ---- the server -------------------------------------------------------- */

/* A request stream, and the bytes the server sends back on it. */
typedef struct {
    int32_t id;
    char path[32];
    uint8_t *out;
    size_t out_len;
    size_t out_sent;
    bool finish; /* send OK trailers once out is sent */
} stream_t;

/* Serves one connection on a thread of its own. Set tls, stall and ping
 * before server_start, and read the rest only after server_stop. */
typedef struct {
    SSL_CTX *tls; /* NULL serves h2c */
    /* Stops reading for STALL_MS once a call opens, with a receive buffer so
     * small that the client's writes block on the socket. */
    bool stall;
    bool stalled;
    /* Sends a PING once PING_AFTER request bytes have arrived, and records
     * when its ACK comes back. */
    bool ping;
    size_t received; /* request bytes on all calls */
    size_t ping_at;  /* received when the PING went out, 0 before */
    size_t ack_at;   /* received when its ACK came back, 0 before */
    uint16_t port;
    int listener;
    zb_thread_t thread;
    bool started;
    int socket;
    SSL *ssl;
    nghttp2_session *session;
    stream_t streams[STREAMS];
    int stream_count;
    char authority[64]; /* the first call's :authority */
    bool hang_up;
    bool break_protocol;
    uint32_t goaway; /* the client's GOAWAY code, or UINT32_MAX */
} server_t;

/* The data source of a response: sends out as it grows, then the trailers. */
static ssize_t read_out(nghttp2_session *session, int32_t stream_id,
                        uint8_t *buf, size_t length, uint32_t *data_flags,
                        nghttp2_data_source *source, void *user_data)
{
    (void)user_data;
    stream_t *stream = source->ptr;
    size_t len = stream->out_len - stream->out_sent;
    if (len > length) {
        len = length;
    }
    if (len != 0) {
        memcpy(buf, stream->out + stream->out_sent, len);
        stream->out_sent += len;
    }
    if (stream->out_sent < stream->out_len) {
        return (ssize_t)len;
    }
    if (!stream->finish) {
        return len != 0 ? (ssize_t)len : NGHTTP2_ERR_DEFERRED;
    }
    *data_flags |= NGHTTP2_DATA_FLAG_EOF | NGHTTP2_DATA_FLAG_NO_END_STREAM;
    return nghttp2_submit_trailer(session, stream_id, ok_trailers, 1) == 0
               ? (ssize_t)len
               : NGHTTP2_ERR_CALLBACK_FAILURE;
}

/* Answers a request once its headers are in. */
static int respond(nghttp2_session *session, server_t *server, stream_t *stream)
{
    nghttp2_data_provider provider = {.source.ptr = stream,
                                      .read_callback = read_out};
    if (strcmp(stream->path, HANG_UP) == 0) {
        server->hang_up = true;
        return 0;
    }
    if (strcmp(stream->path, BREAK) == 0) {
        server->break_protocol = true;
        return 0;
    }
    if (strcmp(stream->path, GO_AWAY) == 0) {
        /* This call is the last the server serves. */
        int result = nghttp2_submit_goaway(
            session, NGHTTP2_FLAG_NONE, stream->id, NGHTTP2_NO_ERROR, NULL, 0);
        if (result != 0) {
            return result;
        }
        stream->finish = true;
    }
    return nghttp2_submit_response(session, stream->id, response, 2, &provider);
}

static int on_begin_headers(nghttp2_session *session,
                            const nghttp2_frame *frame, void *user_data)
{
    server_t *server = user_data;
    if (frame->hd.type == NGHTTP2_HEADERS &&
        frame->headers.cat == NGHTTP2_HCAT_REQUEST &&
        server->stream_count < STREAMS) {
        stream_t *stream = &server->streams[server->stream_count++];
        *stream = (stream_t){.id = frame->hd.stream_id};
        (void)nghttp2_session_set_stream_user_data(session, stream->id, stream);
    }
    return 0;
}

static int on_header(nghttp2_session *session, const nghttp2_frame *frame,
                     const uint8_t *name, size_t name_len, const uint8_t *value,
                     size_t value_len, uint8_t flags, void *user_data)
{
    (void)flags;
    server_t *server = user_data;
    stream_t *stream =
        nghttp2_session_get_stream_user_data(session, frame->hd.stream_id);
    if (stream != NULL && name_len == 5 && memcmp(name, ":path", 5) == 0 &&
        value_len < sizeof(stream->path)) {
        memcpy(stream->path, value, value_len);
    }
    if (name_len == 10 && memcmp(name, ":authority", 10) == 0 &&
        !server->authority[0] && value_len < sizeof(server->authority)) {
        memcpy(server->authority, value, value_len);
    }
    return 0;
}

static int on_data_chunk(nghttp2_session *session, uint8_t flags,
                         int32_t stream_id, const uint8_t *data, size_t len,
                         void *user_data)
{
    (void)flags;
    server_t *server = user_data;
    server->received += len;
    if (server->ping && server->ping_at == 0 &&
        server->received >= PING_AFTER) {
        server->ping_at = server->received;
        if (nghttp2_submit_ping(session, NGHTTP2_FLAG_NONE, NULL) != 0) {
            return NGHTTP2_ERR_CALLBACK_FAILURE;
        }
    }
    /* An echo sends the data back. */
    stream_t *stream = nghttp2_session_get_stream_user_data(session, stream_id);
    if (stream == NULL || strcmp(stream->path, ECHO) != 0 || len == 0) {
        return 0;
    }
    uint8_t *out = realloc(stream->out, stream->out_len + len);
    if (out == NULL) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    memcpy(out + stream->out_len, data, len);
    stream->out = out;
    stream->out_len += len;
    (void)nghttp2_session_resume_data(session, stream_id);
    return 0;
}

static int on_frame_recv(nghttp2_session *session, const nghttp2_frame *frame,
                         void *user_data)
{
    server_t *server = user_data;
    if (frame->hd.type == NGHTTP2_GOAWAY) {
        server->goaway = frame->goaway.error_code;
    } else if (frame->hd.type == NGHTTP2_PING &&
               (frame->hd.flags & NGHTTP2_FLAG_ACK) && server->ack_at == 0) {
        server->ack_at = server->received;
    }
    /* The rest is for HEADERS and DATA frames of request streams. */
    stream_t *stream =
        nghttp2_session_get_stream_user_data(session, frame->hd.stream_id);
    if (stream == NULL ||
        (frame->hd.type != NGHTTP2_HEADERS && frame->hd.type != NGHTTP2_DATA)) {
        return 0;
    }
    if (frame->hd.type == NGHTTP2_HEADERS &&
        respond(session, server, stream) != 0) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
        stream->finish = true;
        (void)nghttp2_session_resume_data(session, stream->id);
    }
    return 0;
}

/* Sends all of data. */
static bool server_send(server_t *server, const uint8_t *data, size_t len)
{
    while (len > 0) {
        ssize_t sent = server->ssl ? SSL_write(server->ssl, data, (int)len)
                                   : write(server->socket, data, len);
        if (sent <= 0) {
            return false;
        }
        data += sent;
        len -= (size_t)sent;
    }
    return true;
}

/* Runs the HTTP/2 session until the client leaves or a call hangs up. */
static void serve_http2(server_t *server)
{
    nghttp2_session_callbacks *callbacks = NULL;
    if (nghttp2_session_callbacks_new(&callbacks) != 0) {
        return;
    }
    nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks,
                                                            on_begin_headers);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks,
                                                              on_data_chunk);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks,
                                                         on_frame_recv);
    int result =
        nghttp2_session_server_new(&server->session, callbacks, server);
    nghttp2_session_callbacks_del(callbacks);
    const nghttp2_settings_entry window = {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE,
                                           LARGE_WINDOW};
    bool large = server->stall || server->ping;
    if (result != 0 ||
        nghttp2_submit_settings(server->session, NGHTTP2_FLAG_NONE, &window,
                                large ? 1 : 0) != 0 ||
        (large && nghttp2_session_set_local_window_size(server->session,
                                                        NGHTTP2_FLAG_NONE, 0,
                                                        LARGE_WINDOW) != 0)) {
        return;
    }
    uint8_t input[16384];
    for (;;) {
        const uint8_t *data = NULL;
        ssize_t len;
        while ((len = nghttp2_session_mem_send(server->session, &data)) > 0) {
            if (!server_send(server, data, (size_t)len)) {
                return;
            }
        }
        if (len < 0 || server->hang_up) {
            return;
        }
        if (server->break_protocol) {
            /* DATA on stream 0, which nghttp2 itself never sends. */
            static const uint8_t frame[] = {0, 0, 1, NGHTTP2_DATA, 0, 0,
                                            0, 0, 0, 'x'};
            server->break_protocol = false;
            if (!server_send(server, frame, sizeof(frame))) {
                return;
            }
        }
        if (server->stall && !server->stalled && server->stream_count > 0) {
            server->stalled = true;
            usleep(STALL_MS * 1000);
        }
        len = server->ssl ? SSL_read(server->ssl, input, sizeof(input))
                          : read(server->socket, input, sizeof(input));
        if (len <= 0 || nghttp2_session_mem_recv(server->session, input,
                                                 (size_t)len) != len) {
            return;
        }
    }
}

static void *serve(void *arg)
{
    server_t *server = arg;
    loopback_block_sigpipe();
    server->socket = accept(server->listener, NULL, NULL);
    if (server->socket < 0) {
        return NULL;
    }
    if (!server->tls || ((server->ssl = SSL_new(server->tls)) != NULL &&
                         SSL_set_fd(server->ssl, server->socket) == 1 &&
                         SSL_accept(server->ssl) == 1)) {
        serve_http2(server);
    }
    nghttp2_session_del(server->session);
    SSL_free(server->ssl);
    close(server->socket);
    return NULL;
}

static bool server_start(server_t *server)
{
    server->goaway = UINT32_MAX;
    server->listener = loopback_listen(&server->port);
    /* A small receive buffer keeps the socket from absorbing the stall. Set
     * before the client connects, so TCP never advertises a larger window. */
    const int buffer = STALL_BUFFER;
    server->started =
        server->listener >= 0 &&
        (!server->stall || setsockopt(server->listener, SOL_SOCKET, SO_RCVBUF,
                                      &buffer, sizeof(buffer)) == 0) &&
        zb_thread_create(&server->thread, serve, server) == ZEROBUS_STATUS_OK;
    if (!server->started && server->listener >= 0) {
        close(server->listener);
    }
    return server->started;
}

/* Stops the server once the client has closed its connection or never
 * connected. Does nothing if the server is not running. */
static void server_stop(server_t *server)
{
    if (!server->started) {
        return;
    }
    server->started = false;
    /* Wakes the server from accept. */
    shutdown(server->listener, SHUT_RDWR);
    (void)zb_thread_join(&server->thread, NULL);
    close(server->listener);
    for (int i = 0; i < server->stream_count; ++i) {
        free(server->streams[i].out);
        server->streams[i].out = NULL;
    }
}

/* ---- the client -------------------------------------------------------- */

/* Opens a transport to the server, over TLS if the server uses it. Each call
 * has room for one received message, so a slow receiver pauses the
 * connection. */
static zerobus_status_t open_transport(const server_t *server,
                                       zb_transport_t **out)
{
    const zb_transport_config_t config = {
        .host = "127.0.0.1",
        .port = server->port,
        .plaintext = server->tls == NULL,
        .tls_context = server->tls != NULL ? trusted : NULL,
        .receive_queue_messages = 1};
    return zb_transport_open(&config, zb_deadline_after_ms(TIMEOUT_MS), out);
}

static zerobus_status_t open_call(zb_transport_t *transport, const char *path,
                                  zb_transport_call_t **out)
{
    return zb_transport_call_open(transport, sv(path), NULL,
                                  zb_deadline_after_ms(TIMEOUT_MS), out);
}

static void message_free(zb_message_t *message)
{
    if (message != NULL) {
        free(message->data);
        free(message);
    }
}

/* Sends a copy of len bytes of data, with len nonzero. */
static zerobus_status_t send_copy(zb_transport_call_t *call, const void *data,
                                  size_t len)
{
    zb_message_t *message = calloc(1, sizeof(*message));
    if (message == NULL || (message->data = malloc(len)) == NULL) {
        message_free(message);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    memcpy(message->data, data, len);
    message->len = len;
    zerobus_status_t status = zb_channel_push(zb_transport_tx(call), message,
                                              zb_deadline_after_ms(TIMEOUT_MS));
    if (status != ZEROBUS_STATUS_OK) {
        message_free(message);
    }
    return status;
}

/* Whether the next message received is len bytes of data. */
static bool received(zb_transport_call_t *call, const void *data, size_t len)
{
    zb_message_t *message = NULL;
    bool match =
        zb_channel_pop(zb_transport_rx(call), zb_deadline_after_ms(TIMEOUT_MS),
                       &message) == ZEROBUS_STATUS_OK &&
        message != NULL && message->len == len &&
        memcmp(message->data, data, len) == 0;
    message_free(message);
    return match;
}

/* Receives until EOF, dropping the messages, and returns the call's status,
 * or the failure of a receive. */
static zerobus_status_t wait_result(zb_transport_call_t *call)
{
    for (;;) {
        zb_message_t *message = NULL;
        zerobus_status_t status = zb_channel_pop(
            zb_transport_rx(call), zb_deadline_after_ms(TIMEOUT_MS), &message);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        if (message == NULL) {
            return zb_transport_get_result(call)->status;
        }
        message_free(message);
    }
}

/* One worker's calls, run one after another on a shared transport. */
typedef struct {
    zb_transport_t *transport;
    int id;
    zb_thread_t thread;
    int succeeded; /* calls that echoed their message and ended OK */
} worker_t;

static void *run_calls(void *arg)
{
    worker_t *worker = arg;
    for (int i = 0; i < CALLS_PER_WORKER; ++i) {
        zb_transport_call_t *call = NULL;
        const uint8_t byte = (uint8_t)(worker->id * CALLS_PER_WORKER + i);
        bool ok =
            open_call(worker->transport, ECHO, &call) == ZEROBUS_STATUS_OK &&
            send_copy(call, &byte, 1) == ZEROBUS_STATUS_OK &&
            zb_transport_half_close(call) == ZEROBUS_STATUS_OK &&
            received(call, &byte, 1) && wait_result(call) == ZEROBUS_STATUS_OK;
        zb_transport_call_free(call);
        worker->succeeded += ok;
    }
    return NULL;
}

/* ---- tests ------------------------------------------------------------- */

/* Bad arguments, and opens that fail before or during connection setup. */
static void test_open_failures(void)
{
    zb_transport_t *transport = NULL;
    zb_transport_call_t *call = NULL;
    int listener = -1;
    zb_deadline_t deadline = zb_deadline_after_ms(TIMEOUT_MS);
    const zb_transport_config_t invalid[] = {
        {.plaintext = true}, /* no host */
        {.host = "", .plaintext = true},
        {.host = "127.0.0.1"}, /* TLS without a context */
        {.host = "127.0.0.1", .plaintext = true, .tls_context = trusted},
        {.host = "127.0.0.1",
         .plaintext = true,
         .send_queue_messages = ZB_SEM_VALUE_MAX},
        /* The host alone is longer than the header limit. */
        {.host = "127.0.0.1", .plaintext = true, .max_header_bytes = 4},
        /* Not a valid :authority. */
        {.host = "a b", .plaintext = true},
    };
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
        CHECK_EQ_INT(zb_transport_open(&invalid[i], deadline, &transport),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
    }
    CHECK_EQ_INT(zb_transport_open(NULL, deadline, &transport),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_transport_call_open(NULL, sv(ECHO), NULL, deadline, &call),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(zb_transport_tx(NULL) == NULL && zb_transport_rx(NULL) == NULL &&
          zb_transport_get_result(NULL) == NULL);
    CHECK_EQ_INT(zb_transport_half_close(NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_transport_cancel(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_transport_shutdown(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    zb_transport_call_free(NULL);
    zb_transport_free(NULL);

    /* Nothing listens on the port. */
    uint16_t port = 0;
    listener = loopback_listen(&port);
    REQUIRE(listener >= 0);
    close(listener);
    zb_transport_config_t config = {
        .host = "127.0.0.1", .port = port, .plaintext = true};
    CHECK_EQ_INT(zb_transport_open(&config, deadline, &transport),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zb_transport_open(&config, deadline, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    /* The deadline passed before the connection's thread would start. */
    CHECK_EQ_INT(zb_transport_open(&config, ZB_DEADLINE_IMMEDIATE, &transport),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);

    /* Nothing accepts, so the TLS handshake gets no answer, and open stops the
     * connection's thread at its deadline. */
    listener = loopback_listen(&port);
    REQUIRE(listener >= 0);
    config = (zb_transport_config_t){
        .host = "127.0.0.1", .port = port, .tls_context = trusted};
    CHECK_EQ_INT(zb_transport_open(&config,
                                   zb_deadline_after_ms(SHORT_TIMEOUT_MS),
                                   &transport),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK(transport == NULL && call == NULL);

zb_cleanup:
    zb_transport_free(transport);
    if (listener >= 0) {
        close(listener);
    }
}

/* Calls on one connection end independently: cancelled, or OK. An echo
 * returns its messages in order, although they arrive while nothing takes
 * them. A call whose open deadline has passed is refused. */
static void test_calls(SSL_CTX *tls)
{
    server_t server = {.tls = tls};
    zb_transport_t *transport = NULL;
    zb_transport_call_t *cancelled = NULL;
    zb_transport_call_t *echo = NULL;
    zb_transport_call_t *late = NULL;
    REQUIRE(server_start(&server));
    REQUIRE_OK(open_transport(&server, &transport));
    REQUIRE_OK(open_call(transport, ECHO, &cancelled));
    REQUIRE_OK(open_call(transport, ECHO, &echo));
    CHECK_EQ_INT(zb_transport_call_open(transport, sv(ECHO), NULL,
                                        ZB_DEADLINE_IMMEDIATE, &late),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK(late == NULL);

    CHECK_OK(send_copy(cancelled, "a", 1));
    CHECK(received(cancelled, "a", 1));
    CHECK_OK(zb_transport_cancel(cancelled));
    CHECK_EQ_INT(wait_result(cancelled), ZEROBUS_STATUS_CANCELLED);
    /* A second cancel changes nothing. */
    CHECK_OK(zb_transport_cancel(cancelled));
    CHECK_EQ_INT(zb_transport_get_result(cancelled)->status,
                 ZEROBUS_STATUS_CANCELLED);
    /* The older call goes first, while the other stays open. */
    zb_transport_call_free(cancelled);
    cancelled = NULL;

    CHECK_OK(send_copy(echo, "a", 1));
    CHECK_OK(send_copy(echo, "b", 1));
    CHECK_OK(zb_transport_half_close(echo));
    usleep(SHORT_TIMEOUT_MS * 1000);
    CHECK(received(echo, "a", 1));
    CHECK(received(echo, "b", 1));
    CHECK_EQ_INT(wait_result(echo), ZEROBUS_STATUS_OK);

    zb_transport_call_free(echo);
    echo = NULL;
    zb_transport_free(transport);
    transport = NULL;
    server_stop(&server);
    /* Calls name the server as host:port. */
    char authority[32];
    (void)snprintf(authority, sizeof(authority), "127.0.0.1:%u",
                   (unsigned)server.port);
    CHECK(strcmp(server.authority, authority) == 0);

zb_cleanup:
    zb_transport_call_free(echo);
    zb_transport_call_free(cancelled);
    zb_transport_free(transport);
    server_stop(&server);
}

/* An upload the server receives in full. While a stalling server stops
 * reading, the client's writes block on the socket and later resume. A
 * pinging server's PING, sent partway through, is answered long before the
 * upload ends, so the client reads while it writes. */
static void test_upload(SSL_CTX *tls, bool stall)
{
    server_t server = {.tls = tls, .stall = stall, .ping = !stall};
    zb_transport_t *transport = NULL;
    zb_transport_call_t *call = NULL;
    uint8_t *big = calloc(1, BIG_MESSAGE);
    REQUIRE(big != NULL);
    REQUIRE(server_start(&server));
    REQUIRE_OK(open_transport(&server, &transport));
    REQUIRE_OK(open_call(transport, SINK, &call));

    for (int i = 0; i < UPLOAD_MESSAGES; ++i) {
        CHECK_OK(send_copy(call, big, BIG_MESSAGE));
    }
    CHECK_OK(zb_transport_half_close(call));
    CHECK_EQ_INT(wait_result(call), ZEROBUS_STATUS_OK);
    zb_transport_free(transport);
    transport = NULL;
    call = NULL;
    server_stop(&server);
    /* Each message has a 5-byte prefix. */
    CHECK_EQ_INT(server.received, UPLOAD_MESSAGES * (BIG_MESSAGE + 5));
    CHECK(stall ||
          (server.ack_at != 0 && server.ack_at - server.ping_at < PING_SLACK));

zb_cleanup:
    zb_transport_call_free(call);
    zb_transport_free(transport);
    server_stop(&server);
    free(big);
}

/* The connection ends when the call on path makes the server hang up or
 * break the protocol, or with path NULL, when the transport shuts down. Every
 * call on it ends with status, and the transport refuses new calls with it.
 * Free releases the calls still open. */
static void test_connection_end(const char *path, zerobus_status_t status)
{
    server_t server = {0};
    zb_transport_t *transport = NULL;
    zb_transport_call_t *echo = NULL;
    zb_transport_call_t *call = NULL;
    zb_transport_call_t *late = NULL;
    REQUIRE(server_start(&server));
    REQUIRE_OK(open_transport(&server, &transport));
    REQUIRE_OK(open_call(transport, ECHO, &echo));
    if (path != NULL) {
        REQUIRE_OK(open_call(transport, path, &call));
        CHECK_EQ_INT(wait_result(call), status);
    } else {
        CHECK_OK(zb_transport_shutdown(transport));
        CHECK_OK(zb_transport_shutdown(transport));
    }
    CHECK_EQ_INT(wait_result(echo), status);
    CHECK_EQ_INT(open_call(transport, ECHO, &late), status);
    CHECK(late == NULL);

    zb_transport_free(transport);
    transport = NULL;
    echo = call = NULL;
    server_stop(&server);
    /* After a protocol error, the client's GOAWAY still reached the server
     * before the connection closed. */
    CHECK(status != ZEROBUS_STATUS_INTERNAL ||
          server.goaway == NGHTTP2_PROTOCOL_ERROR);

zb_cleanup:
    zb_transport_call_free(call);
    zb_transport_call_free(echo);
    zb_transport_free(transport);
    server_stop(&server);
}

/* After the server's GOAWAY, the call it still serves ends OK. Then the
 * connection is over, and the transport refuses new calls. */
static void test_goaway(void)
{
    server_t server = {0};
    zb_transport_t *transport = NULL;
    zb_transport_call_t *call = NULL;
    zb_transport_call_t *late = NULL;
    REQUIRE(server_start(&server));
    REQUIRE_OK(open_transport(&server, &transport));
    REQUIRE_OK(open_call(transport, GO_AWAY, &call));

    CHECK_EQ_INT(wait_result(call), ZEROBUS_STATUS_OK);
    CHECK_EQ_INT(open_call(transport, ECHO, &late), ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(late == NULL);

zb_cleanup:
    zb_transport_call_free(late);
    zb_transport_call_free(call);
    zb_transport_free(transport);
    server_stop(&server);
}

/* Workers on several threads open, use and free calls on one connection at
 * the same time. */
static void test_concurrent_workers(void)
{
    server_t server = {0};
    zb_transport_t *transport = NULL;
    worker_t workers[WORKERS] = {0};
    int started = 0;
    REQUIRE(server_start(&server));
    REQUIRE_OK(open_transport(&server, &transport));
    for (; started < WORKERS; ++started) {
        workers[started].transport = transport;
        workers[started].id = started;
        REQUIRE_OK(zb_thread_create(&workers[started].thread, run_calls,
                                    &workers[started]));
    }

zb_cleanup:
    for (int i = 0; i < started; ++i) {
        (void)zb_thread_join(&workers[i].thread, NULL);
        CHECK_EQ_INT(workers[i].succeeded, CALLS_PER_WORKER);
    }
    zb_transport_free(transport);
    server_stop(&server);
}

int main(void)
{
    if (!loopback_write_certificate(CERT_FILE, KEY_FILE)) {
        fprintf(stderr, "  FAIL: could not write the test certificate\n");
        return 1;
    }
    SSL_CTX *tls = NULL;
    REQUIRE_OK(zb_tls_ctx_new(CERT_FILE, &trusted));
    tls = loopback_server_tls(CERT_FILE, KEY_FILE, true);
    REQUIRE(tls != NULL);
    test_open_failures();
    test_calls(NULL);
    test_calls(tls);
    test_upload(tls, true);
    test_upload(NULL, false);
    test_connection_end(HANG_UP, ZEROBUS_STATUS_UNAVAILABLE);
    test_connection_end(BREAK, ZEROBUS_STATUS_INTERNAL);
    test_connection_end(NULL, ZEROBUS_STATUS_CANCELLED);
    test_goaway();
    test_concurrent_workers();

zb_cleanup:
    SSL_CTX_free(tls);
    zb_tls_ctx_free(trusted);
    TEST_MAIN_RETURN();
}
