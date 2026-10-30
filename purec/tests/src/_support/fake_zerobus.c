#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <nghttp2/nghttp2.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "fake_zerobus.h"

#define MAX_CONNECTIONS 64
#define TOKEN "fake-token"

/* One EphemeralStream call. */
typedef struct call {
    struct call *next;
    int32_t id;
    char path[128];
    char authorization[256];
    bool responded; /* Response headers submitted. */
    bool finishing; /* Trailers go out once out is drained. */
    bool deferred;
    uint32_t grpc_status;
    unsigned index; /* Order among all calls. */
    int64_t expected;
    uint64_t received, unacked;
    uint8_t *in;
    size_t in_len, in_cap;
    uint8_t *out;
    size_t out_len, out_pos, out_cap;
} call_t;

typedef struct {
    struct fake_zerobus *server;
    int fd;
    bool http1;
    nghttp2_session *session;
    call_t *calls;
    uint8_t *pending;
    size_t pending_len;
    char request[16384];
    size_t request_len;
} connection_t;

struct fake_zerobus {
    fake_zerobus_options_t options;
    int listener, token_listener, wake[2];
    uint16_t port, token_port;
    pthread_t thread;
    connection_t connections[MAX_CONNECTIONS];
    atomic_uint_fast64_t records;
    atomic_uint_fast64_t digest;
    atomic_uint calls, tokens;
};

/* ---- helpers ---------------------------------------------------------- */

static void append(uint8_t **data, size_t *len, size_t *cap, const void *src,
                   size_t n)
{
    if (*len + n > *cap) {
        size_t grown = *cap ? *cap * 2 : 4096;
        while (grown < *len + n) {
            grown *= 2;
        }
        *data = realloc(*data, grown);
        *cap = grown;
    }
    memcpy(*data + *len, src, n);
    *len += n;
}

static size_t put_varint(uint8_t *out, uint64_t value)
{
    size_t n = 0;
    while (value >= 0x80) {
        out[n++] = (uint8_t)(value | 0x80);
        value >>= 7;
    }
    out[n++] = (uint8_t)value;
    return n;
}

static bool get_varint(const uint8_t **p, const uint8_t *end, uint64_t *out)
{
    uint64_t value = 0;
    for (unsigned shift = 0; *p < end && shift < 64; shift += 7) {
        uint8_t byte = *(*p)++;
        value |= (uint64_t)(byte & 0x7f) << shift;
        if (!(byte & 0x80)) {
            *out = value;
            return true;
        }
    }
    return false;
}

/* Finds length-delimited field number field in message, or a varint one with
 * varint set. */
static bool find_field(const uint8_t *p, const uint8_t *end, unsigned field,
                       const uint8_t **out, size_t *out_len, uint64_t *varint)
{
    while (p < end) {
        uint64_t key, value;
        if (!get_varint(&p, end, &key)) {
            return false;
        }
        unsigned type = (unsigned)(key & 7);
        if (type == 0) {
            if (!get_varint(&p, end, &value)) {
                return false;
            }
            if ((key >> 3) == field && varint) {
                *varint = value;
                return true;
            }
        } else if (type == 2) {
            if (!get_varint(&p, end, &value) || value > (uint64_t)(end - p)) {
                return false;
            }
            if ((key >> 3) == field && out) {
                *out = p;
                *out_len = (size_t)value;
                return true;
            }
            p += value;
        } else {
            return false;
        }
    }
    return false;
}

/* ---- HTTP/2 calls ------------------------------------------------------ */

static call_t *find_call(connection_t *connection, int32_t id)
{
    for (call_t *call = connection->calls; call; call = call->next) {
        if (call->id == id) {
            return call;
        }
    }
    return NULL;
}

static ssize_t read_out(nghttp2_session *session, int32_t id, uint8_t *buffer,
                        size_t length, uint32_t *flags,
                        nghttp2_data_source *source, void *user_data)
{
    (void)user_data;
    call_t *call = source->ptr;
    size_t n = call->out_len - call->out_pos;
    if (n > length) {
        n = length;
    }
    if (n > 0) {
        memcpy(buffer, call->out + call->out_pos, n);
    }
    call->out_pos += n;
    if (call->out_pos == call->out_len) {
        call->out_pos = call->out_len = 0;
    }
    if (n == 0) {
        if (!call->finishing) {
            call->deferred = true;
            return NGHTTP2_ERR_DEFERRED;
        }
        char status[16];
        snprintf(status, sizeof(status), "%u", (unsigned)call->grpc_status);
        nghttp2_nv trailer = {(uint8_t *)"grpc-status", (uint8_t *)status, 11,
                              strlen(status), NGHTTP2_NV_FLAG_NONE};
        *flags |= NGHTTP2_DATA_FLAG_EOF | NGHTTP2_DATA_FLAG_NO_END_STREAM;
        nghttp2_submit_trailer(session, id, &trailer, 1);
    }
    return (ssize_t)n;
}

static void respond(connection_t *connection, call_t *call)
{
    if (!call->responded) {
        nghttp2_nv headers[] = {{(uint8_t *)":status", (uint8_t *)"200", 7, 3,
                                 NGHTTP2_NV_FLAG_NONE},
                                {(uint8_t *)"content-type",
                                 (uint8_t *)"application/grpc", 12, 16,
                                 NGHTTP2_NV_FLAG_NONE}};
        nghttp2_data_provider provider = {{.ptr = call}, read_out};
        nghttp2_submit_response(connection->session, call->id, headers, 2,
                                &provider);
        call->responded = true;
    } else if (call->deferred) {
        call->deferred = false;
        nghttp2_session_resume_data(connection->session, call->id);
    }
}

/* Queues one response message. */
static void send_message(connection_t *connection, call_t *call,
                         const uint8_t *data, size_t len)
{
    uint8_t prefix[5] = {0, (uint8_t)(len >> 24), (uint8_t)(len >> 16),
                         (uint8_t)(len >> 8), (uint8_t)len};
    append(&call->out, &call->out_len, &call->out_cap, prefix, 5);
    append(&call->out, &call->out_len, &call->out_cap, data, len);
    respond(connection, call);
}

static void send_ack(connection_t *connection, call_t *call)
{
    uint8_t inner[11], message[16];
    inner[0] = 0x08;
    size_t inner_len =
        1 + put_varint(inner + 1, (uint64_t)(call->expected - 1));
    message[0] = 0x12;
    message[1] = (uint8_t)inner_len;
    memcpy(message + 2, inner, inner_len);
    send_message(connection, call, message, inner_len + 2);
    call->unacked = 0;
}

/* Ends the call with status once queued messages are out. */
static void finish(connection_t *connection, call_t *call, uint32_t status)
{
    if (call->finishing) {
        return;
    }
    call->finishing = true;
    call->grpc_status = status;
    respond(connection, call);
}

static void handle_message(connection_t *connection, call_t *call,
                           const uint8_t *data, size_t len)
{
    fake_zerobus_t *server = connection->server;
    const uint8_t *body = NULL, *end = data + len;
    size_t body_len = 0;
    if (call->finishing) {
        return;
    }
    if (find_field(data, end, 1, &body, &body_len, NULL)) {
        char id[32];
        int id_len = snprintf(id, sizeof(id), "fake-%u", call->index);
        uint8_t message[48] = {0x0a, (uint8_t)(id_len + 2), 0x0a,
                               (uint8_t)id_len};
        memcpy(message + 4, id, (size_t)id_len);
        send_message(connection, call, message, (size_t)id_len + 4);
        return;
    }
    bool batch = find_field(data, end, 3, &body, &body_len, NULL);
    if (!batch && !find_field(data, end, 2, &body, &body_len, NULL)) {
        finish(connection, call, 3);
        return;
    }
    uint64_t offset = UINT64_MAX;
    if (!find_field(body, body + body_len, 1, NULL, NULL, &offset) ||
        (int64_t)offset != call->expected) {
        finish(connection, call, 3); /* INVALID_ARGUMENT */
        return;
    }
    uint64_t records = 1;
    uint64_t digest = atomic_load(&server->digest);
    const uint8_t *json = NULL;
    size_t json_len = 0;
    bool found = find_field(body, body + body_len, 3, &json, &json_len, NULL);
    if (batch) {
        records = 0;
        if (found) {
            const uint8_t *p = json, *json_end = json + json_len;
            const uint8_t *record;
            size_t record_len;
            while (p < json_end &&
                   find_field(p, json_end, 1, &record, &record_len, NULL)) {
                ++records;
                digest = fake_zerobus_digest_add(digest, record, record_len);
                p = record + record_len;
            }
        }
    } else if (found) {
        digest = fake_zerobus_digest_add(digest, json, json_len);
    }
    atomic_store(&server->digest, digest);
    call->expected++;
    call->received += records;
    call->unacked++;
    atomic_fetch_add(&server->records, records);
    unsigned every = server->options.ack_every ? server->options.ack_every : 1;
    if (server->options.fail_after &&
        call->index <= server->options.fail_calls &&
        call->received >= server->options.fail_after) {
        finish(connection, call, 14); /* UNAVAILABLE, last records unacked */
        return;
    }
    if (call->unacked >= every) {
        send_ack(connection, call);
    }
}

static int on_begin_headers(nghttp2_session *session,
                            const nghttp2_frame *frame, void *user_data)
{
    (void)session;
    connection_t *connection = user_data;
    if (frame->hd.type == NGHTTP2_HEADERS &&
        frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        call_t *call = calloc(1, sizeof(*call));
        call->id = frame->hd.stream_id;
        call->next = connection->calls;
        connection->calls = call;
    }
    return 0;
}

static int on_header(nghttp2_session *session, const nghttp2_frame *frame,
                     const uint8_t *name, size_t name_len, const uint8_t *value,
                     size_t value_len, uint8_t flags, void *user_data)
{
    (void)session;
    (void)flags;
    call_t *call = find_call(user_data, frame->hd.stream_id);
    if (!call) {
        return 0;
    }
    char *target = name_len == 5 && !memcmp(name, ":path", 5) ? call->path
                   : name_len == 13 && !memcmp(name, "authorization", 13)
                       ? call->authorization
                       : NULL;
    if (target && value_len < 128) {
        memcpy(target, value, value_len);
        target[value_len] = '\0';
    }
    return 0;
}

static int on_data_chunk(nghttp2_session *session, uint8_t flags, int32_t id,
                         const uint8_t *data, size_t len, void *user_data)
{
    (void)session;
    (void)flags;
    connection_t *connection = user_data;
    call_t *call = find_call(connection, id);
    if (!call) {
        return 0;
    }
    append(&call->in, &call->in_len, &call->in_cap, data, len);
    size_t used = 0;
    while (call->in_len - used >= 5) {
        const uint8_t *p = call->in + used;
        size_t message_len =
            (size_t)p[1] << 24 | (size_t)p[2] << 16 | (size_t)p[3] << 8 | p[4];
        if (call->in_len - used - 5 < message_len) {
            break;
        }
        handle_message(connection, call, p + 5, message_len);
        used += 5 + message_len;
    }
    memmove(call->in, call->in + used, call->in_len - used);
    call->in_len -= used;
    return 0;
}

/* Orders the call and checks its headers, before any of its DATA. */
static void admit(connection_t *connection, call_t *call)
{
    fake_zerobus_t *server = connection->server;
    call->index = atomic_fetch_add(&server->calls, 1) + 1;
    const char *required = server->options.authorization;
    if (strcmp(call->path, "/databricks.zerobus.Zerobus/EphemeralStream")) {
        finish(connection, call, 12); /* UNIMPLEMENTED */
    } else if (required && strcmp(required, call->authorization)) {
        finish(connection, call, 16); /* UNAUTHENTICATED */
    } else if (server->options.refuse_status) {
        finish(connection, call, server->options.refuse_status);
    }
}

static int on_frame_recv(nghttp2_session *session, const nghttp2_frame *frame,
                         void *user_data)
{
    (void)session;
    connection_t *connection = user_data;
    call_t *call = find_call(connection, frame->hd.stream_id);
    if (!call) {
        return 0;
    }
    if (frame->hd.type == NGHTTP2_HEADERS &&
        frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        admit(connection, call);
    }
    if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
        if (call->unacked && !call->finishing) {
            send_ack(connection, call);
        }
        finish(connection, call, 0);
    }
    return 0;
}

static int on_stream_close(nghttp2_session *session, int32_t id,
                           uint32_t error_code, void *user_data)
{
    (void)session;
    (void)error_code;
    connection_t *connection = user_data;
    for (call_t **link = &connection->calls; *link; link = &(*link)->next) {
        if ((*link)->id == id) {
            call_t *call = *link;
            *link = call->next;
            free(call->in);
            free(call->out);
            free(call);
            break;
        }
    }
    return 0;
}

/* ---- connections ------------------------------------------------------- */

static void close_connection(connection_t *connection)
{
    nghttp2_session_del(connection->session);
    while (connection->calls) {
        call_t *call = connection->calls;
        connection->calls = call->next;
        free(call->in);
        free(call->out);
        free(call);
    }
    free(connection->pending);
    close(connection->fd);
    memset(connection, 0, sizeof(*connection));
    connection->fd = -1;
}

/* Writes what nghttp2 has queued; false when the connection must close. */
static bool flush_h2(connection_t *connection)
{
    for (;;) {
        if (connection->pending_len == 0) {
            const uint8_t *data;
            ssize_t len = nghttp2_session_mem_send(connection->session, &data);
            if (len < 0) {
                return false;
            }
            if (len == 0) {
                return true;
            }
            connection->pending = realloc(connection->pending, (size_t)len);
            memcpy(connection->pending, data, (size_t)len);
            connection->pending_len = (size_t)len;
        }
        ssize_t n = send(connection->fd, connection->pending,
                         connection->pending_len, MSG_NOSIGNAL);
        if (n < 0) {
            return errno == EAGAIN || errno == EWOULDBLOCK;
        }
        memmove(connection->pending, connection->pending + n,
                connection->pending_len - (size_t)n);
        connection->pending_len -= (size_t)n;
        if (connection->pending_len) {
            return true;
        }
    }
}

static void accept_h2(fake_zerobus_t *server, connection_t *connection, int fd)
{
    nghttp2_session_callbacks *callbacks;
    nghttp2_session_callbacks_new(&callbacks);
    nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks,
                                                            on_begin_headers);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks,
                                                              on_data_chunk);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks,
                                                         on_frame_recv);
    nghttp2_session_callbacks_set_on_stream_close_callback(callbacks,
                                                           on_stream_close);
    connection->server = server;
    connection->fd = fd;
    nghttp2_session_server_new(&connection->session, callbacks, connection);
    nghttp2_session_callbacks_del(callbacks);
    nghttp2_settings_entry settings[] = {
        {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 1 << 20}};
    nghttp2_submit_settings(connection->session, NGHTTP2_FLAG_NONE, settings,
                            1);
}

/* Reads an HTTP/1.1 token request and answers it once complete. The
 * response nests authorization_details before expires_in, as staging does. */
static bool serve_token(fake_zerobus_t *server, connection_t *connection)
{
    ssize_t n =
        recv(connection->fd, connection->request + connection->request_len,
             sizeof(connection->request) - 1 - connection->request_len, 0);
    if (n <= 0) {
        return n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
    }
    connection->request_len += (size_t)n;
    connection->request[connection->request_len] = '\0';
    char *end = strstr(connection->request, "\r\n\r\n");
    char *length = strcasestr(connection->request, "content-length:");
    if (!end || !length ||
        (size_t)(connection->request + connection->request_len - (end + 4)) <
            strtoul(length + 15, NULL, 10)) {
        return true;
    }
    static const char body[] =
        "{\"access_token\":\"" TOKEN "\",\"authorization_details\":[{\"type\":"
        "\"unity_catalog_privileges\",\"privileges\":[\"SELECT\",\"MODIFY\"],"
        "\"object_full_path\":\"c.s.t\"}],\"scope\":\"all-apis\","
        "\"token_type\":\"Bearer\",\"expires_in\":3600}";
    char response[1024];
    int len = snprintf(response, sizeof(response),
                       "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                       "Content-Length: %zu\r\nConnection: close\r\n\r\n%s",
                       sizeof(body) - 1, body);
    (void)send(connection->fd, response, (size_t)len, MSG_NOSIGNAL);
    atomic_fetch_add(&server->tokens, 1);
    return false;
}

/* ---- server ------------------------------------------------------------ */

static int listen_on(uint16_t *out_port)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    struct sockaddr_in address = {.sin_family = AF_INET,
                                  .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t size = sizeof(address);
    if (fd < 0 || bind(fd, (struct sockaddr *)&address, size) != 0 ||
        listen(fd, 64) != 0 ||
        getsockname(fd, (struct sockaddr *)&address, &size) != 0) {
        if (fd >= 0) {
            close(fd);
        }
        return -1;
    }
    *out_port = ntohs(address.sin_port);
    return fd;
}

static void accept_on(fake_zerobus_t *server, int listener, bool http1)
{
    int fd = accept4(listener, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0) {
        return;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    for (int i = 0; i < MAX_CONNECTIONS; ++i) {
        connection_t *connection = &server->connections[i];
        if (connection->fd < 0) {
            if (http1) {
                connection->fd = fd;
                connection->http1 = true;
            } else {
                accept_h2(server, connection, fd);
            }
            return;
        }
    }
    close(fd);
}

static void *serve(void *arg)
{
    fake_zerobus_t *server = arg;
    for (;;) {
        struct pollfd fds[MAX_CONNECTIONS + 3];
        int map[MAX_CONNECTIONS + 3];
        nfds_t count = 0;
        fds[count++] = (struct pollfd){server->wake[0], POLLIN, 0};
        fds[count++] = (struct pollfd){server->listener, POLLIN, 0};
        fds[count++] = (struct pollfd){server->token_listener, POLLIN, 0};
        for (int i = 0; i < MAX_CONNECTIONS; ++i) {
            connection_t *connection = &server->connections[i];
            if (connection->fd >= 0) {
                short events = POLLIN;
                if (connection->pending_len ||
                    (connection->session &&
                     nghttp2_session_want_write(connection->session))) {
                    events |= POLLOUT;
                }
                map[count] = i;
                fds[count++] = (struct pollfd){connection->fd, events, 0};
            }
        }
        if (poll(fds, count, 1000) < 0 && errno != EINTR) {
            break;
        }
        if (fds[0].revents) {
            break;
        }
        if (fds[1].revents & POLLIN) {
            accept_on(server, server->listener, false);
        }
        if (fds[2].revents & POLLIN) {
            accept_on(server, server->token_listener, true);
        }
        for (nfds_t f = 3; f < count; ++f) {
            connection_t *connection = &server->connections[map[f]];
            bool keep = true;
            if (connection->http1) {
                if (fds[f].revents) {
                    keep = serve_token(server, connection);
                }
            } else {
                if (fds[f].revents & (POLLIN | POLLHUP | POLLERR)) {
                    uint8_t buffer[65536];
                    ssize_t n = recv(connection->fd, buffer, sizeof(buffer), 0);
                    if (n > 0) {
                        keep = nghttp2_session_mem_recv(connection->session,
                                                        buffer, (size_t)n) >= 0;
                    } else if (n == 0 || (errno != EAGAIN && errno != EINTR)) {
                        keep = false;
                    }
                }
                keep = keep && flush_h2(connection);
                keep =
                    keep && (nghttp2_session_want_read(connection->session) ||
                             nghttp2_session_want_write(connection->session));
            }
            if (!keep) {
                close_connection(connection);
            }
        }
    }
    return NULL;
}

zerobus_status_t fake_zerobus_start(const fake_zerobus_options_t *options,
                                    fake_zerobus_t **out_server)
{
    fake_zerobus_t *server = calloc(1, sizeof(*server));
    if (!server) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    if (options) {
        server->options = *options;
    }
    atomic_init(&server->digest, FAKE_ZEROBUS_DIGEST_SEED);
    for (int i = 0; i < MAX_CONNECTIONS; ++i) {
        server->connections[i].fd = -1;
    }
    server->listener = listen_on(&server->port);
    server->token_listener = listen_on(&server->token_port);
    if (server->listener < 0 || server->token_listener < 0 ||
        pipe2(server->wake, O_CLOEXEC) != 0 ||
        pthread_create(&server->thread, NULL, serve, server) != 0) {
        free(server);
        return ZEROBUS_STATUS_UNAVAILABLE;
    }
    *out_server = server;
    return ZEROBUS_STATUS_OK;
}

uint16_t fake_zerobus_port(const fake_zerobus_t *server)
{
    return server->port;
}

uint16_t fake_zerobus_token_port(const fake_zerobus_t *server)
{
    return server->token_port;
}

uint64_t fake_zerobus_records(fake_zerobus_t *server)
{
    return atomic_load(&server->records);
}

uint64_t fake_zerobus_digest(fake_zerobus_t *server)
{
    return atomic_load(&server->digest);
}

uint64_t fake_zerobus_digest_add(uint64_t digest, const void *data, size_t len)
{
    const uint8_t *bytes = data;
    for (size_t i = 0; i < len; ++i) {
        digest = (digest ^ bytes[i]) * UINT64_C(1099511628211);
    }
    return digest;
}

unsigned fake_zerobus_calls(fake_zerobus_t *server)
{
    return atomic_load(&server->calls);
}

unsigned fake_zerobus_tokens(fake_zerobus_t *server)
{
    return atomic_load(&server->tokens);
}

void fake_zerobus_stop(fake_zerobus_t *server)
{
    if (!server) {
        return;
    }
    (void)!write(server->wake[1], "x", 1);
    pthread_join(server->thread, NULL);
    for (int i = 0; i < MAX_CONNECTIONS; ++i) {
        if (server->connections[i].fd >= 0) {
            close_connection(&server->connections[i]);
        }
    }
    close(server->listener);
    close(server->token_listener);
    close(server->wake[0]);
    close(server->wake[1]);
    free(server);
}
