#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "http.h"
#include "internal/common/utils.h"

typedef struct {
    zb_socket_t socket;
    zb_tls_t *tls;
    zb_wakeup_t *wakeup;
    zb_deadline_t deadline;
} connection_t;

/* A response being read: raw bytes, and once headers end, how to find the
 * end of the body. */
typedef struct {
    char *data;
    size_t len;
    size_t capacity;
    size_t body_start; /* 0 until the header block is complete. */
    uint32_t status;
    bool chunked;
    bool has_length;
    size_t content_length;
} reader_t;

static zerobus_status_t wait_for(connection_t *connection, unsigned want)
{
    if (zb_deadline_expired(connection->deadline)) {
        return ZEROBUS_STATUS_DEADLINE_EXCEEDED;
    }
    zb_net_interest_t interest = {connection->socket, want,
                                  connection->deadline};
    unsigned events = 0;
    zerobus_status_t status =
        zb_net_wait(&interest, connection->wakeup, &events);
    if (status == ZEROBUS_STATUS_OK && events == 0 &&
        zb_deadline_expired(connection->deadline)) {
        return ZEROBUS_STATUS_DEADLINE_EXCEEDED;
    }
    return status;
}

static zerobus_status_t open_connection(connection_t *connection,
                                        const zb_tls_ctx_t *tls,
                                        const char *host, uint16_t port)
{
    zb_net_addresses_t *addresses = NULL;
    zerobus_status_t status = zb_net_resolve(host, port, &addresses);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    bool connected = false;
    while (!connected) {
        status =
            zb_socket_connect_next(addresses, &connection->socket, &connected);
        if (status != ZEROBUS_STATUS_OK) {
            break;
        }
        if (!connected) {
            status = wait_for(connection, ZB_NET_WRITE);
            if (status == ZEROBUS_STATUS_OK) {
                status = zb_socket_finish_connect(connection->socket);
            }
            if (status == ZEROBUS_STATUS_OK) {
                connected = true;
            } else {
                zb_socket_close(connection->socket);
                connection->socket = ZB_SOCKET_INVALID;
                if (status == ZEROBUS_STATUS_DEADLINE_EXCEEDED) {
                    break;
                }
            }
        }
    }
    zb_net_addresses_free(addresses);
    if (!connected) {
        return status;
    }
    if (!tls) {
        return ZEROBUS_STATUS_OK;
    }
    status = zb_tls_new(tls, connection->socket, host, false, &connection->tls);
    bool ready = false;
    while (status == ZEROBUS_STATUS_OK && !ready) {
        unsigned want = 0;
        status = zb_tls_handshake(connection->tls, &want, &ready);
        if (status == ZEROBUS_STATUS_OK && !ready) {
            status = wait_for(connection, want);
        }
    }
    return status;
}

static zerobus_status_t write_all(connection_t *connection, const char *data,
                                  size_t len)
{
    while (len != 0) {
        zb_io_result_t result;
        zerobus_status_t status =
            connection->tls
                ? zb_tls_write(connection->tls, (const uint8_t *)data, len,
                               &result)
                : zb_socket_write(connection->socket, (const uint8_t *)data,
                                  len, &result);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        if (result.want) {
            status = wait_for(connection, result.want);
            if (status != ZEROBUS_STATUS_OK) {
                return status;
            }
            continue;
        }
        data += result.bytes;
        len -= result.bytes;
    }
    return ZEROBUS_STATUS_OK;
}

/* Case-insensitive match of a header name at line against name. */
static bool header_is(const char *line, size_t len, const char *name)
{
    size_t name_len = strlen(name);
    return len > name_len && line[name_len] == ':' &&
           zb_starts_with(line, name_len, name);
}

static const char *header_value(const char *line, size_t len, size_t *out_len)
{
    const char *value = memchr(line, ':', len) + 1;
    const char *end = line + len;
    while (value < end && (*value == ' ' || *value == '\t')) {
        ++value;
    }
    while (end > value && (end[-1] == ' ' || end[-1] == '\t')) {
        --end;
    }
    *out_len = (size_t)(end - value);
    return value;
}

/* Parses the status line and the headers that frame the body, once the
 * header block is complete. */
static zerobus_status_t parse_head(reader_t *reader)
{
    const char *end = NULL;
    for (size_t i = 3; i < reader->len; ++i) {
        if (memcmp(reader->data + i - 3, "\r\n\r\n", 4) == 0) {
            end = reader->data + i + 1;
            break;
        }
    }
    if (!end) {
        return ZEROBUS_STATUS_OK;
    }
    const char *line = reader->data;
    const char *line_end = strstr(line, "\r\n");
    if (line_end - line < 12 || memcmp(line, "HTTP/1.", 7) != 0 ||
        line[8] != ' ' ||
        !zb_parse_u32((const uint8_t *)line + 9, 3, &reader->status)) {
        return ZEROBUS_STATUS_UNAVAILABLE;
    }
    for (line = line_end + 2; line < end - 2; line = line_end + 2) {
        line_end = strstr(line, "\r\n");
        size_t len = (size_t)(line_end - line);
        size_t value_len = 0;
        if (header_is(line, len, "content-length")) {
            const char *value = header_value(line, len, &value_len);
            uint32_t length = 0;
            if (!zb_parse_u32((const uint8_t *)value, value_len, &length)) {
                return ZEROBUS_STATUS_UNAVAILABLE;
            }
            reader->has_length = true;
            reader->content_length = length;
        } else if (header_is(line, len, "transfer-encoding")) {
            const char *value = header_value(line, len, &value_len);
            reader->chunked =
                value_len >= 7 &&
                zb_starts_with(value + value_len - 7, 7, "chunked");
        }
    }
    reader->body_start = (size_t)(end - reader->data);
    return ZEROBUS_STATUS_OK;
}

/* Walks the chunked body at p. Returns 1 once the last chunk has arrived,
 * 0 while more input is needed, and -1 for a malformed body. *out_len
 * receives the decoded length; out, when non-NULL, receives the bytes. Chunk
 * extensions and trailers are ignored. */
static int walk_chunks(const char *p, size_t len, char *out, size_t *out_len)
{
    size_t i = 0, total = 0;
    for (;;) {
        size_t size = 0, digits = 0;
        while (i < len && zb_hex_value(p[i]) >= 0) {
            if (size > ZB_HTTP_MAX_RESPONSE) {
                return -1;
            }
            size = size * 16 + (size_t)zb_hex_value(p[i]);
            ++i;
            ++digits;
        }
        while (i < len && p[i] != '\r') {
            ++i;
        }
        if (i + 1 >= len) {
            return 0;
        }
        if (digits == 0 || p[i + 1] != '\n') {
            return -1;
        }
        i += 2;
        if (size == 0) {
            *out_len = total;
            return 1;
        }
        if (len - i < size + 2) {
            return 0;
        }
        if (out) {
            memcpy(out + total, p + i, size);
        }
        total += size;
        i += size;
        if (p[i] != '\r' || p[i + 1] != '\n') {
            return -1;
        }
        i += 2;
    }
}

/* Reads until the response is complete and sets *out_body_len to the length
 * of its decoded body. */
static zerobus_status_t read_response(connection_t *connection,
                                      reader_t *reader, size_t *out_body_len)
{
    for (;;) {
        if (reader->body_start) {
            const char *body = reader->data + reader->body_start;
            size_t body_len = reader->len - reader->body_start;
            if (reader->chunked) {
                int done = walk_chunks(body, body_len, NULL, out_body_len);
                if (done < 0) {
                    return ZEROBUS_STATUS_UNAVAILABLE;
                }
                if (done > 0) {
                    return ZEROBUS_STATUS_OK;
                }
            } else if (reader->has_length &&
                       body_len >= reader->content_length) {
                *out_body_len = reader->content_length;
                return ZEROBUS_STATUS_OK;
            }
        }
        if (reader->len + 4096 > reader->capacity) {
            if (reader->capacity >= ZB_HTTP_MAX_RESPONSE + 65536) {
                return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
            }
            size_t capacity = reader->capacity ? reader->capacity * 2 : 16384;
            char *data = realloc(reader->data, capacity + 1);
            if (!data) {
                return ZEROBUS_STATUS_OUT_OF_MEMORY;
            }
            reader->data = data;
            reader->capacity = capacity;
        }
        zb_io_result_t result;
        uint8_t *into = (uint8_t *)reader->data + reader->len;
        size_t room = reader->capacity - reader->len;
        zerobus_status_t status =
            connection->tls
                ? zb_tls_read(connection->tls, into, room, &result)
                : zb_socket_read(connection->socket, into, room, &result);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        if (result.eof) {
            /* Without a length the body ends with the connection. */
            if (reader->body_start && !reader->chunked && !reader->has_length) {
                *out_body_len = reader->len - reader->body_start;
                return ZEROBUS_STATUS_OK;
            }
            return ZEROBUS_STATUS_UNAVAILABLE;
        }
        if (result.want) {
            status = wait_for(connection, result.want);
            if (status != ZEROBUS_STATUS_OK) {
                return status;
            }
            continue;
        }
        reader->len += result.bytes;
        reader->data[reader->len] = '\0';
        if (!reader->body_start) {
            status = parse_head(reader);
            if (status != ZEROBUS_STATUS_OK) {
                return status;
            }
        }
    }
}

zerobus_status_t zb_http_post(const zb_tls_ctx_t *tls, const char *host,
                              uint16_t port, const char *path,
                              const char *headers, const char *content_type,
                              const uint8_t *body, size_t body_len,
                              zb_deadline_t deadline, zb_http_response_t *out)
{
    if (!host || !path || !headers || !content_type || (!body && body_len) ||
        !out) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    *out = (zb_http_response_t){0};
    connection_t connection = {.socket = ZB_SOCKET_INVALID,
                               .deadline = deadline};
    reader_t reader = {0};
    char *request = NULL;
    zerobus_status_t status = zb_wakeup_new(&connection.wakeup);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    status = open_connection(&connection, tls, host, port);
    if (status != ZEROBUS_STATUS_OK) {
        goto done;
    }
    const char *format = "POST %s HTTP/1.1\r\nHost: %s%s%.0u\r\n"
                         "Content-Type: %s\r\nContent-Length: %zu\r\n"
                         "Connection: close\r\n%s\r\n";
    bool default_port = port == (tls ? 443 : 80);
    int head_len =
        snprintf(NULL, 0, format, path, host, default_port ? "" : ":",
                 default_port ? 0u : port, content_type, body_len, headers);
    if (head_len < 0 || (size_t)head_len > SIZE_MAX - body_len - 1) {
        status = ZEROBUS_STATUS_INVALID_ARGUMENT;
        goto done;
    }
    request = malloc((size_t)head_len + body_len + 1);
    if (!request) {
        status = ZEROBUS_STATUS_OUT_OF_MEMORY;
        goto done;
    }
    (void)snprintf(request, (size_t)head_len + 1, format, path, host,
                   default_port ? "" : ":", default_port ? 0u : port,
                   content_type, body_len, headers);
    if (body_len) {
        memcpy(request + head_len, body, body_len);
    }
    status = write_all(&connection, request, (size_t)head_len + body_len);
    zb_secure_free(request, (size_t)head_len + body_len);
    request = NULL;
    if (status != ZEROBUS_STATUS_OK) {
        goto done;
    }
    size_t response_len = 0;
    status = read_response(&connection, &reader, &response_len);
    if (status != ZEROBUS_STATUS_OK) {
        goto done;
    }
    if (response_len > ZB_HTTP_MAX_RESPONSE) {
        status = ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
        goto done;
    }
    out->body = malloc(response_len + 1);
    if (!out->body) {
        status = ZEROBUS_STATUS_OUT_OF_MEMORY;
        goto done;
    }
    const char *raw = reader.data + reader.body_start;
    if (reader.chunked) {
        (void)walk_chunks(raw, reader.len - reader.body_start, out->body,
                          &response_len);
    } else {
        memcpy(out->body, raw, response_len);
    }
    out->body[response_len] = '\0';
    out->body_len = response_len;
    out->status = reader.status;
done:
    if (reader.data) {
        zb_secure_free(reader.data, reader.capacity + 1);
    }
    zb_tls_free(connection.tls);
    zb_socket_close(connection.socket);
    zb_wakeup_free(connection.wakeup);
    return status;
}

void zb_http_response_clear(zb_http_response_t *response)
{
    if (response) {
        if (response->body) {
            zb_secure_free(response->body, response->body_len + 1);
        }
        *response = (zb_http_response_t){0};
    }
}
