/* HTTP client tests against an in-process server on a loopback port. */
#define _GNU_SOURCE

#include <openssl/ssl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "headers_provider.h"
#include "http_server.h"
#include "internal/network/http_client.h"
#include "loopback.h"
#include "test_common.h"

#define CERT_FILE ZB_TEST_DIR "/cert.pem"
#define KEY_FILE ZB_TEST_DIR "/key.pem"
#define PATH "/oidc/v1/token"
#define BODY "grant_type=client_credentials"

enum {
    TIMEOUT_MS = 5000,
    SHORT_TIMEOUT_MS = 10,
    /* Early Hints responses that take far longer than SHORT_TIMEOUT_MS to
     * read. */
    EARLY_HINTS = 300000,
};

/* ---- helpers ----------------------------------------------------------- */

/* Posts BODY to PATH on the server with one extra header. */
static zerobus_status_t post(const http_server_t *server,
                             const zb_tls_ctx_t *tls, uint64_t timeout_ms,
                             zb_http_response_t *out)
{
    char origin[64];
    snprintf(origin, sizeof(origin), "%s://127.0.0.1:%u",
             tls ? "https" : "http", (unsigned)server->port);
    zb_header_t header = {"Authorization", "Basic aWQ6c2VjcmV0"};
    const zerobus_headers_t headers = {.items = &header, .count = 1};
    return zb_http_client_post(tls, origin, PATH, &headers, BODY, strlen(BODY),
                               zb_deadline_after_ms(timeout_ms), out);
}

/* Runs one exchange with a plain HTTP server sending reply, and returns the
 * client's status. */
static zerobus_status_t exchange(const char *const *reply, bool hold,
                                 zb_http_response_t *out)
{
    http_server_t server = {.hold = hold};
    memcpy(server.reply, reply, sizeof(server.reply));
    if (!http_server_start(&server)) {
        return ZEROBUS_STATUS_INTERNAL;
    }
    zerobus_status_t status = post(&server, NULL, TIMEOUT_MS, out);
    http_server_stop(&server);
    return status;
}

/* Returns prefix followed by count copies of fill, or NULL. */
static char *repeat(const char *prefix, char fill, size_t count)
{
    size_t len = strlen(prefix);
    char *text = malloc(len + count + 1);
    if (text) {
        memcpy(text, prefix, len);
        memset(text + len, fill, count);
        text[len + count] = '\0';
    }
    return text;
}

/* Returns count 103 Early Hints responses followed by an empty 200, or
 * NULL. */
static char *early_hints(size_t count)
{
    static const char hint[] = "HTTP/1.1 103 Early Hints\r\n\r\n";
    static const char final[] = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    size_t hint_len = sizeof(hint) - 1;
    char *text = malloc(count * hint_len + sizeof(final));
    if (text) {
        for (size_t i = 0; i < count; ++i) {
            memcpy(text + i * hint_len, hint, hint_len);
        }
        memcpy(text + count * hint_len, final, sizeof(final));
    }
    return text;
}

/* ---- tests ------------------------------------------------------------- */

static void test_arguments(void)
{
    zb_header_t no_name = {"", "c"};
    zb_header_t bad_name = {"a:b", "c"};
    zb_header_t bad_value = {"a", "b\r\nc: d"};
    const zerobus_headers_t none = {0};
    const zerobus_headers_t bad_headers[] = {
        {.items = &no_name, .count = 1},
        {.items = &bad_name, .count = 1},
        {.items = &bad_value, .count = 1},
    };
    zb_http_response_t response;
    zb_deadline_t deadline = ZB_DEADLINE_IMMEDIATE;
    const char *origin = "http://127.0.0.1:1";

    CHECK_EQ_INT(zb_http_client_post(NULL, NULL, PATH, &none, NULL, 0, deadline,
                                     &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_http_client_post(NULL, "http://host/path", PATH, &none,
                                     NULL, 0, deadline, &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_http_client_post(NULL, "https://host", PATH, &none, NULL, 0,
                                     deadline, &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_http_client_post(NULL, origin, "token", &none, NULL, 0,
                                     deadline, &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_http_client_post(NULL, origin, "/a b", &none, NULL, 0,
                                     deadline, &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_http_client_post(NULL, origin, PATH, NULL, NULL, 0,
                                     deadline, &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    for (size_t i = 0; i < sizeof(bad_headers) / sizeof(bad_headers[0]); ++i) {
        CHECK_EQ_INT(zb_http_client_post(NULL, origin, PATH, &bad_headers[i],
                                         NULL, 0, deadline, &response),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
    }
    CHECK_EQ_INT(zb_http_client_post(NULL, origin, PATH, &none, NULL, 1,
                                     deadline, &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_http_client_post(NULL, origin, PATH, &none, "x", SIZE_MAX,
                                     deadline, &response),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zb_http_client_post(NULL, origin, PATH, &none, NULL, 0, deadline, NULL),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    zb_http_response_clear(NULL);
}

static void test_request(void)
{
    zb_http_response_t response = {0};
    http_server_t server = {
        .reply = {"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"}};
    REQUIRE(http_server_start(&server));
    CHECK_OK(post(&server, NULL, TIMEOUT_MS, &response));
    http_server_stop(&server);

    char expected[512];
    snprintf(expected, sizeof(expected),
             "POST " PATH " HTTP/1.1\r\n"
             "Host: 127.0.0.1:%u\r\n"
             "Content-Type: application/x-www-form-urlencoded\r\n"
             "Content-Length: %zu\r\n"
             "Connection: close\r\n"
             "Authorization: Basic aWQ6c2VjcmV0\r\n"
             "\r\n" BODY,
             (unsigned)server.port, strlen(BODY));
    CHECK(strcmp(server.request, expected) == 0);
    CHECK_EQ_INT(response.status, 200);
    CHECK_EQ_INT(response.body_len, 2);
    CHECK(response.body && strcmp(response.body, "{}") == 0);
    zb_http_response_clear(&response);
    CHECK(response.body == NULL);

zb_cleanup:
    zb_http_response_clear(&response);
}

static void test_responses(void)
{
    static const struct {
        const char *reply[HTTP_SERVER_MAX_PIECES];
        bool hold; /* The client must not wait for the server to close. */
        uint32_t status;
        const char *body;
    } cases[] = {
        {{"HTTP/1.1 400 Bad Request\r\nContent-", "Length:  5 \r\n\r\nhel",
          "lo"},
         true,
         400,
         "hello"},
        {{"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n",
          "Content-Length: 5\r\n\r\nhel", "lo"},
         true,
         200,
         "hello"},
        {{"HTTP/1.0 200\r\n\r\nhel", "lo"}, false, 200, "hello"},
        {{"HTTP/1.1 204 No Content\r\n\r\n"}, true, 204, ""},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        zb_http_response_t response = {0};
        CHECK_OK(exchange(cases[i].reply, cases[i].hold, &response));
        CHECK_EQ_INT(response.status, cases[i].status);
        CHECK(response.body && strcmp(response.body, cases[i].body) == 0);
        zb_http_response_clear(&response);
    }
}

static void test_malformed(void)
{
    static const struct {
        const char *reply;
        zerobus_status_t status;
    } cases[] = {
        {"HTTP/2 200\r\n\r\n", ZEROBUS_STATUS_INTERNAL},
        {"HTTP/1.1 2OO OK\r\n\r\n", ZEROBUS_STATUS_INTERNAL},
        {"HTTP/1.1 200 OK\r\nno colon\r\n\r\n", ZEROBUS_STATUS_INTERNAL},
        {"HTTP/1.1 200 OK\r\nContent-Length: \r\n\r\n",
         ZEROBUS_STATUS_INTERNAL},
        {"HTTP/1.1 200 OK\r\nContent-Length: x\r\n\r\n",
         ZEROBUS_STATUS_INTERNAL},
        {"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n",
         ZEROBUS_STATUS_INTERNAL},
        /* Cut short: the server closes before the body is complete. */
        {"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nhello",
         ZEROBUS_STATUS_UNAVAILABLE},
        {"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
         ZEROBUS_STATUS_UNIMPLEMENTED},
        {"HTTP/1.1 200 OK\r\nContent-Length: 1048577\r\n\r\n",
         ZEROBUS_STATUS_OUT_OF_RANGE},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        zb_http_response_t response = {0};
        CHECK_EQ_INT(
            exchange((const char *[HTTP_SERVER_MAX_PIECES]){cases[i].reply},
                     false, &response),
            cases[i].status);
        zb_http_response_clear(&response);
    }
}

static void test_limits(void)
{
    zb_http_response_t response = {0};
    char *largest = repeat("HTTP/1.1 200 OK\r\nContent-Length: 1048576\r\n\r\n",
                           'x', ZB_HTTP_CLIENT_MAX_BODY);
    char *too_long =
        repeat("HTTP/1.1 200 OK\r\n\r\n", 'x', ZB_HTTP_CLIENT_MAX_BODY + 1);
    char *long_head = repeat("HTTP/1.1 200 OK\r\nx: ", 'x', 64 * 1024);
    REQUIRE(largest && too_long && long_head);

    CHECK_OK(exchange((const char *[HTTP_SERVER_MAX_PIECES]){largest}, false,
                      &response));
    CHECK_EQ_INT(response.body_len, ZB_HTTP_CLIENT_MAX_BODY);
    zb_http_response_clear(&response);
    CHECK_EQ_INT(exchange((const char *[HTTP_SERVER_MAX_PIECES]){too_long},
                          false, &response),
                 ZEROBUS_STATUS_OUT_OF_RANGE);
    CHECK_EQ_INT(exchange((const char *[HTTP_SERVER_MAX_PIECES]){long_head},
                          false, &response),
                 ZEROBUS_STATUS_OUT_OF_RANGE);

zb_cleanup:
    zb_http_response_clear(&response);
    free(largest);
    free(too_long);
    free(long_head);
}

/* The deadline holds against a server that never answers, and against one
 * that keeps the client reading with Early Hints responses. */
static void test_deadline(void)
{
    zb_http_response_t response = {0};
    http_server_t silent = {.hold = true};
    http_server_t flooding = {.reply = {early_hints(EARLY_HINTS)}};
    REQUIRE(flooding.reply[0] != NULL);
    REQUIRE(http_server_start(&silent));
    CHECK_EQ_INT(post(&silent, NULL, SHORT_TIMEOUT_MS, &response),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    http_server_stop(&silent);

    REQUIRE(http_server_start(&flooding));
    CHECK_EQ_INT(post(&flooding, NULL, SHORT_TIMEOUT_MS, &response),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    http_server_stop(&flooding);

zb_cleanup:
    zb_http_response_clear(&response);
    free((char *)flooding.reply[0]);
}

/* Without a port, the host is the whole authority. A label over 63 characters
 * fails inside libc, before any DNS query. */
static void test_origin_without_port(void)
{
    zb_http_response_t response;
    char origin[96] = "http://";
    memset(origin + 7, 'a', 64);
    strcpy(origin + 71, ".invalid");
    const zerobus_headers_t none = {0};
    CHECK_EQ_INT(zb_http_client_post(NULL, origin, PATH, &none, NULL, 0,
                                     zb_deadline_after_ms(TIMEOUT_MS),
                                     &response),
                 ZEROBUS_STATUS_UNAVAILABLE);
}

static void test_https(void)
{
    zb_http_response_t response = {0};
    zb_tls_ctx_t *tls = NULL;
    http_server_t server = {
        .reply = {"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"},
        .tls = loopback_server_tls(CERT_FILE, KEY_FILE, false)};
    REQUIRE(server.tls != NULL);
    REQUIRE_OK(zb_tls_ctx_new(CERT_FILE, &tls));
    REQUIRE(http_server_start(&server));
    CHECK_OK(post(&server, tls, TIMEOUT_MS, &response));
    http_server_stop(&server);
    CHECK(strstr(server.request, "\r\n\r\n" BODY) != NULL);
    CHECK_EQ_INT(response.status, 200);
    CHECK(response.body && strcmp(response.body, "{}") == 0);

zb_cleanup:
    zb_http_response_clear(&response);
    zb_tls_ctx_free(tls);
    SSL_CTX_free(server.tls);
}

int main(void)
{
    if (!loopback_write_certificate(CERT_FILE, KEY_FILE)) {
        fprintf(stderr, "  FAIL: could not write the test certificate\n");
        return 1;
    }
    test_arguments();
    test_request();
    test_responses();
    test_malformed();
    test_limits();
    test_deadline();
    test_origin_without_port();
    test_https();
    TEST_MAIN_RETURN();
}
