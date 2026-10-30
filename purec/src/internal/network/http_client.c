#include "internal/network/http_client.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "headers_provider.h"
#include "internal/common/utils.h"
#include "internal/network/connection.h"
#include "internal/network/grpc_utils.h"

#define MAX_HEAD (64u * 1024u)
#define MAX_BUFFER (MAX_HEAD + ZB_HTTP_CLIENT_MAX_BODY + 1u)
#define MIN_BUFFER (16u * 1024u)

/* One request and its response on a connection of their own. */
typedef struct {
    zb_connection_t connection;
    zb_wakeup_t *wakeup; /* Never signaled, but connection waits need one. */
    zb_deadline_t deadline;
} exchange_t;

/* How the end of the response body is found. */
typedef enum {
    BODY_EMPTY,  /* There is no body. */
    BODY_LENGTH, /* After content_length bytes. */
    BODY_CLOSE   /* When the server closes the connection. */
} framing_t;

/* A response being read. The head fills data[0, head_len), and the body
 * follows it. */
typedef struct {
    char *data;
    size_t len;
    size_t capacity;
    size_t scanned;  /* Bytes searched for the end of the head. */
    size_t head_len; /* 0 until the final head is complete. */
    uint32_t status;
    framing_t framing;
    size_t content_length;
    size_t body_len;
    bool done;
} response_t;

/* ---- text ------------------------------------------------------------- */

/* Returns the first CRLF in [p, end), or NULL. */
static const char *find_crlf(const char *p, const char *end)
{
    while (p < end && (p = memchr(p, '\r', (size_t)(end - p))) != NULL) {
        if (end - p >= 2 && p[1] == '\n') {
            return p;
        }
        ++p;
    }
    return NULL;
}

/* ---- request ------------------------------------------------------------ */

/* Keeps each header on a line of its own, with its name before the ':'. */
static bool headers_are_valid(const zerobus_headers_t *headers)
{
    if (!headers) {
        return false;
    }
    for (size_t i = 0; i < headers->count; ++i) {
        const char *name = headers->items[i].name;
        const char *value = headers->items[i].value;
        if (!name || !name[0] || strchr(name, ':') || !value ||
            !zb_is_header_value(zerobus_string_view(name)) ||
            !zb_is_header_value(zerobus_string_view(value))) {
            return false;
        }
    }
    return true;
}

static void put(char **cursor, const char *data, size_t len)
{
    memcpy(*cursor, data, len);
    *cursor += len;
}

/* Formats the request into *out, which the caller releases with
 * zb_secure_free(*out, *out_len). */
static zerobus_status_t format_request(const char *authority, const char *path,
                                       const zerobus_headers_t *headers,
                                       const char *body, size_t body_len,
                                       char **out, size_t *out_len)
{
    static const char format[] = "POST %s HTTP/1.1\r\nHost: %s\r\n"
                                 "Content-Length: %zu\r\n"
                                 "Connection: close\r\n";
    int start = snprintf(NULL, 0, format, path, authority, body_len);
    if (start < 0) {
        return ZEROBUS_STATUS_INTERNAL;
    }
    size_t len = (size_t)start + 2;
    for (size_t i = 0; i < headers->count; ++i) {
        const zb_header_t *header = &headers->items[i];
        len += strlen(header->name) + 2 + strlen(header->value) + 2;
    }
    if (body_len >= SIZE_MAX - len) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    len += body_len;
    char *request = malloc(len + 1);
    if (!request) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    (void)snprintf(request, (size_t)start + 1, format, path, authority,
                   body_len);
    char *cursor = request + start;
    for (size_t i = 0; i < headers->count; ++i) {
        const zb_header_t *header = &headers->items[i];
        put(&cursor, header->name, strlen(header->name));
        put(&cursor, ": ", 2);
        put(&cursor, header->value, strlen(header->value));
        put(&cursor, "\r\n", 2);
    }
    put(&cursor, "\r\n", 2);
    if (body_len) {
        put(&cursor, body, body_len);
    }
    *out = request;
    *out_len = len;
    return ZEROBUS_STATUS_OK;
}

/* ---- connection --------------------------------------------------------- */

static zerobus_status_t write_all(exchange_t *exchange, const char *data,
                                  size_t len)
{
    while (len) {
        /* Waits check the deadline only when the socket is not ready, so a
         * transfer that keeps going checks it here. */
        if (zb_deadline_expired(exchange->deadline)) {
            return ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        }
        zb_io_result_t result;
        zerobus_status_t status = zb_connection_write(
            &exchange->connection, (const uint8_t *)data, len, &result);
        if (status == ZEROBUS_STATUS_OK && result.want) {
            status = zb_connection_wait(&exchange->connection, result.want,
                                        exchange->wakeup, exchange->deadline);
        } else if (status == ZEROBUS_STATUS_OK) {
            data += result.bytes;
            len -= result.bytes;
        }
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
    }
    return ZEROBUS_STATUS_OK;
}

/* Reads more of the response, growing its buffer up to MAX_BUFFER. Sets
 * *out_eof once the server has closed the connection. */
static zerobus_status_t read_more(exchange_t *exchange, response_t *response,
                                  bool *out_eof)
{
    /* As in write_all, for a server that keeps sending. */
    if (zb_deadline_expired(exchange->deadline)) {
        return ZEROBUS_STATUS_DEADLINE_EXCEEDED;
    }
    if (response->len == response->capacity) {
        if (response->capacity == MAX_BUFFER) {
            return ZEROBUS_STATUS_OUT_OF_RANGE;
        }
        size_t capacity =
            response->capacity ? 2 * response->capacity : MIN_BUFFER;
        if (capacity > MAX_BUFFER) {
            capacity = MAX_BUFFER;
        }
        /* Not realloc: the old buffer must be zeroed before it is freed. */
        char *data = malloc(capacity);
        if (!data) {
            return ZEROBUS_STATUS_OUT_OF_MEMORY;
        }
        if (response->len) {
            memcpy(data, response->data, response->len);
        }
        zb_secure_free(response->data, response->capacity);
        response->data = data;
        response->capacity = capacity;
    }
    for (;;) {
        uint8_t *into = (uint8_t *)response->data + response->len;
        size_t room = response->capacity - response->len;
        zb_io_result_t result;
        zerobus_status_t status =
            zb_connection_read(&exchange->connection, into, room, &result);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        if (!result.want) {
            response->len += result.bytes;
            *out_eof = result.eof;
            return ZEROBUS_STATUS_OK;
        }
        status = zb_connection_wait(&exchange->connection, result.want,
                                    exchange->wakeup, exchange->deadline);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
    }
}

/* ---- response ----------------------------------------------------------- */

/* Parses "HTTP/1.x NNN", followed by a space and a reason or by nothing. */
static bool parse_status_line(const char *line, size_t len,
                              uint32_t *out_status)
{
    return len >= 12 && memcmp(line, "HTTP/1.", 7) == 0 && line[7] >= '0' &&
           line[7] <= '9' && line[8] == ' ' && (len == 12 || line[12] == ' ') &&
           zb_parse_u32((const uint8_t *)line + 9, 3, out_status) &&
           *out_status >= 100;
}

static zerobus_status_t parse_content_length(zerobus_string_view_t value,
                                             size_t *out_length)
{
    uint32_t length = 0;
    if (!zb_parse_u32((const uint8_t *)value.data, value.len, &length)) {
        return ZEROBUS_STATUS_INTERNAL;
    }
    if (length > ZB_HTTP_CLIENT_MAX_BODY) {
        return ZEROBUS_STATUS_OUT_OF_RANGE;
    }
    *out_length = length;
    return ZEROBUS_STATUS_OK;
}

/* Parses the complete head in data[0, head_len): the status and how the body
 * is framed. */
static zerobus_status_t parse_head(response_t *response, size_t head_len)
{
    /* The head ends with an empty line, at end. */
    const char *end = response->data + head_len - 2;
    const char *line_end = find_crlf(response->data, end);
    if (!parse_status_line(response->data, (size_t)(line_end - response->data),
                           &response->status)) {
        return ZEROBUS_STATUS_INTERNAL;
    }
    bool has_length = false;
    size_t length = 0;
    for (const char *line = line_end + 2; line < end; line = line_end + 2) {
        line_end = find_crlf(line, end);
        const char *colon = memchr(line, ':', (size_t)(line_end - line));
        if (!colon || colon == line) {
            return ZEROBUS_STATUS_INTERNAL;
        }
        zerobus_string_view_t value = zb_trim_whitespace(
            (zerobus_string_view_t){colon + 1, (size_t)(line_end - colon - 1)});
        size_t name_len = (size_t)(colon - line);
        if (zb_equals_ignore_case(line, name_len, "content-length")) {
            size_t parsed = 0;
            zerobus_status_t status = parse_content_length(value, &parsed);
            if (status != ZEROBUS_STATUS_OK) {
                return status;
            }
            if (has_length && parsed != length) {
                return ZEROBUS_STATUS_INTERNAL;
            }
            has_length = true;
            length = parsed;
        } else if (zb_equals_ignore_case(line, name_len, "transfer-encoding")) {
            /* Token responses carry Content-Length instead. */
            return ZEROBUS_STATUS_UNIMPLEMENTED;
        }
    }
    if (response->status == 204 || response->status == 304) {
        response->framing = BODY_EMPTY;
    } else if (has_length) {
        response->framing = BODY_LENGTH;
        response->content_length = length;
    } else {
        response->framing = BODY_CLOSE;
    }
    return ZEROBUS_STATUS_OK;
}

/* Parses the final head once it has arrived, dropping interim 1xx
 * responses. Leaves head_len at 0 while more input is needed. */
static zerobus_status_t parse_final_head(response_t *response)
{
    for (;;) {
        size_t i = response->scanned;
        while (i + 4 <= response->len &&
               memcmp(response->data + i, "\r\n\r\n", 4) != 0) {
            ++i;
        }
        if (i + 4 > response->len) {
            response->scanned = i;
            return response->len >= MAX_HEAD ? ZEROBUS_STATUS_OUT_OF_RANGE
                                             : ZEROBUS_STATUS_OK;
        }
        size_t head_len = i + 4;
        zerobus_status_t status = parse_head(response, head_len);
        if (status != ZEROBUS_STATUS_OK || response->status >= 200) {
            response->head_len = head_len;
            return status;
        }
        memmove(response->data, response->data + head_len,
                response->len - head_len);
        response->len -= head_len;
        response->scanned = 0;
    }
}

/* Parses what has arrived so far, and sets done once the response is
 * complete. */
static zerobus_status_t parse_response(response_t *response)
{
    if (!response->head_len) {
        zerobus_status_t status = parse_final_head(response);
        if (status != ZEROBUS_STATUS_OK || !response->head_len) {
            return status;
        }
    }
    size_t received = response->len - response->head_len;
    switch (response->framing) {
    case BODY_EMPTY:
        response->done = true;
        break;
    case BODY_LENGTH:
        if (received >= response->content_length) {
            response->body_len = response->content_length;
            response->done = true;
        }
        break;
    case BODY_CLOSE:
        if (received > ZB_HTTP_CLIENT_MAX_BODY) {
            return ZEROBUS_STATUS_OUT_OF_RANGE;
        }
        break;
    }
    return ZEROBUS_STATUS_OK;
}

static zerobus_status_t read_response(exchange_t *exchange,
                                      response_t *response)
{
    for (;;) {
        zerobus_status_t status = parse_response(response);
        if (status != ZEROBUS_STATUS_OK || response->done) {
            return status;
        }
        bool eof = false;
        status = read_more(exchange, response, &eof);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        if (eof) {
            if (!response->head_len || response->framing != BODY_CLOSE) {
                return ZEROBUS_STATUS_UNAVAILABLE;
            }
            response->body_len = response->len - response->head_len;
            return ZEROBUS_STATUS_OK;
        }
    }
}

/* ---- entry points ------------------------------------------------------- */

zerobus_status_t zb_http_client_post(const zb_tls_ctx_t *tls,
                                     const char *origin, const char *path,
                                     const zerobus_headers_t *headers,
                                     const char *body, size_t body_len,
                                     zb_deadline_t deadline,
                                     zb_http_response_t *out)
{
    if (!origin || zb_url_validate(zerobus_string_view(origin)) != ZB_URL_OK ||
        !path || !zb_is_valid_grpc_target(zerobus_string_view(path), false) ||
        !headers_are_valid(headers) || (!body && body_len) || !out) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    const char *authority = strstr(origin, "://") + 3;
    bool https = authority - origin == 8;
    if (https && !tls) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    *out = (zb_http_response_t){0};
    const char *colon = strchr(authority, ':');
    uint16_t port = colon   ? (uint16_t)strtoul(colon + 1, NULL, 10)
                    : https ? 443
                            : 80;

    exchange_t exchange = {.connection = ZB_CONNECTION_CLOSED,
                           .deadline = deadline};
    response_t response = {0};
    char *request = NULL;
    size_t request_len = 0;
    char *host = zb_strndup(authority, colon ? (size_t)(colon - authority)
                                             : strlen(authority));
    zerobus_status_t status = ZEROBUS_STATUS_OUT_OF_MEMORY;
    if (host) {
        status = format_request(authority, path, headers, body, body_len,
                                &request, &request_len);
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = zb_wakeup_new(&exchange.wakeup);
    }
    if (status == ZEROBUS_STATUS_OK) {
        status =
            zb_connection_open(&exchange.connection, https ? tls : NULL, host,
                               port, false, exchange.wakeup, deadline);
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = write_all(&exchange, request, request_len);
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = read_response(&exchange, &response);
    }
    if (status == ZEROBUS_STATUS_OK) {
        out->body =
            zb_strndup(response.data + response.head_len, response.body_len);
        if (out->body) {
            out->status = response.status;
            out->body_len = response.body_len;
        } else {
            status = ZEROBUS_STATUS_OUT_OF_MEMORY;
        }
    }
    zb_secure_free(response.data, response.capacity);
    zb_secure_free(request, request_len);
    zb_connection_close(&exchange.connection);
    zb_wakeup_free(exchange.wakeup);
    free(host);
    return status;
}

void zb_http_response_clear(zb_http_response_t *response)
{
    if (!response) {
        return;
    }
    zb_secure_free(response->body, response->body_len);
    *response = (zb_http_response_t){0};
}
