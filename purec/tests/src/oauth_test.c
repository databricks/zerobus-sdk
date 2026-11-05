/* Unit tests for zerobus_get_token and zerobus_oauth_token_free (oauth.c),
 * against a Unity Catalog token endpoint played by the scripted HTTP server on
 * a loopback port. */
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "http_server.h"
#include "internal/common/utils.h"
#include "loopback.h"
#include "test_common.h"

/* ---- helpers ----------------------------------------------------------- */

static uint64_t unix_time_ms(void)
{
    struct timespec now = {0};
    (void)timespec_get(&now, TIME_UTC);
    return (uint64_t)now.tv_sec * 1000u + (uint64_t)now.tv_nsec / 1000000u;
}

/* Gets a token for table c.s.t in workspace 123, as client "id" with secret
 * "secret", from server, which answers with status_line and body. Returns the
 * call's status, or UNKNOWN if the server did not start. The server has
 * stopped by then, so its fields can be read. */
static zerobus_status_t get_token(http_server_t *server,
                                  const char *status_line, const char *body,
                                  zerobus_oauth_token_t *out_token,
                                  zerobus_error_t **out_error)
{
    char reply[512];
    snprintf(reply, sizeof(reply),
             "HTTP/1.1 %s\r\nContent-Length: %zu\r\n\r\n%s", status_line,
             strlen(body), body);
    server->reply[0] = reply;
    if (!http_server_start(server)) {
        return ZEROBUS_STATUS_UNKNOWN;
    }
    char origin[32];
    snprintf(origin, sizeof(origin), "http://127.0.0.1:%u",
             (unsigned)server->port);
    zerobus_status_t status =
        zerobus_get_token(sv(origin), sv("c.s.t"), sv("id"), sv("secret"),
                          sv("123"), out_token, out_error);
    http_server_stop(server);
    return status;
}

/* ---- tests ------------------------------------------------------------- */

/* The token expires its lifetime after the request was sent. Freeing it
 * leaves it empty, and freeing again or NULL does nothing. */
static void test_get_token(void)
{
    http_server_t server = {0};
    zerobus_oauth_token_t token = {0};

    uint64_t before = unix_time_ms();
    REQUIRE_OK(get_token(&server, "200 OK",
                         "{\"access_token\":\"tok\",\"expires_in\":3600}",
                         &token, NULL));
    uint64_t after = unix_time_ms();
    CHECK(strcmp(token.access_token, "tok") == 0);
    CHECK(token.expires_at_unix_ms >= before + 3600000u &&
          token.expires_at_unix_ms <= after + 3600000u);
    CHECK(strncmp(server.request, "POST /oidc/v1/token HTTP/1.1\r\n", 30) == 0);
    CHECK(strstr(server.request, "\r\nauthorization: Basic aWQ6c2VjcmV0\r\n"));
    CHECK(strstr(server.request, "workspaces%2F123%2FzerobusDirectWriteApi"));
    CHECK(strstr(server.request, "%22c.s.t%22"));

zb_cleanup:
    zerobus_oauth_token_free(&token);
    CHECK(token.access_token == NULL && token.expires_at_unix_ms == 0);
    zerobus_oauth_token_free(&token);
    zerobus_oauth_token_free(NULL);
}

/* A response without a lifetime gives no expiry. */
static void test_no_expiry(void)
{
    http_server_t server = {0};
    zerobus_oauth_token_t token = {NULL, 7};

    REQUIRE_OK(get_token(&server, "200 OK", "{\"access_token\":\"tok\"}",
                         &token, NULL));
    CHECK(strcmp(token.access_token, "tok") == 0);
    CHECK_EQ_INT(token.expires_at_unix_ms, 0);

zb_cleanup:
    zerobus_oauth_token_free(&token);
}

/* A 5xx response may succeed on retry, and a 4xx one will not. Either leaves
 * the token untouched. */
static void test_http_errors(void)
{
    static const struct {
        const char *status_line;
        const char *body;
        zerobus_status_t status;
        bool retryable;
        const char *message;
    } cases[] = {
        {"401 Unauthorized", "{\"error\":\"invalid_client\"}",
         ZEROBUS_STATUS_UNAUTHENTICATED, false,
         "token request failed with HTTP status 401: "
         "{\"error\":\"invalid_client\"}"},
        {"503 Service Unavailable", "", ZEROBUS_STATUS_UNAVAILABLE, true,
         "token request failed with HTTP status 503"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        http_server_t server = {0};
        zerobus_oauth_token_t token = {NULL, 7};
        zerobus_error_t *error = NULL;
        CHECK_EQ_INT(get_token(&server, cases[i].status_line, cases[i].body,
                               &token, &error),
                     cases[i].status);
        CHECK(zerobus_error_is_retryable(error) == cases[i].retryable);
        CHECK(zb_view_equals(zerobus_error_message(error), cases[i].message));
        CHECK(token.access_token == NULL && token.expires_at_unix_ms == 7);
        zerobus_error_free(error);
    }
}

/* An endpoint that refuses the connection may accept a retry, over http or
 * https. */
static void test_unreachable(void)
{
    static const char *const schemes[] = {"http", "https"};
    zerobus_error_t *error = NULL;
    uint16_t port = 0;
    int listener = loopback_listen(&port);
    REQUIRE(listener >= 0);
    close(listener);
    for (size_t i = 0; i < sizeof(schemes) / sizeof(schemes[0]); ++i) {
        char origin[40];
        snprintf(origin, sizeof(origin), "%s://127.0.0.1:%u", schemes[i],
                 (unsigned)port);
        zerobus_oauth_token_t token = {NULL, 7};
        CHECK_EQ_INT(zerobus_get_token(sv(origin), sv("c.s.t"), sv("id"),
                                       sv("secret"), sv("123"), &token, &error),
                     ZEROBUS_STATUS_UNAVAILABLE);
        CHECK(zerobus_error_is_retryable(error));
        CHECK(token.access_token == NULL && token.expires_at_unix_ms == 7);
        zerobus_error_free(error);
        error = NULL;
    }

zb_cleanup:
    zerobus_error_free(error);
}

/* Each invalid argument is rejected for good, without a request, leaving the
 * token untouched. */
static void test_invalid_arguments(void)
{
    http_server_t server = {0};
    zerobus_oauth_token_t token = {NULL, 7};
    zerobus_error_t *error = NULL;
    server.reply[0] = "HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\n"
                      "{\"access_token\":\"t\"}";
    REQUIRE(http_server_start(&server));
    char origin[32];
    char with_path[40];
    snprintf(origin, sizeof(origin), "http://127.0.0.1:%u",
             (unsigned)server.port);
    snprintf(with_path, sizeof(with_path), "%s/", origin);
    const zerobus_string_view_t valid[] = {sv(origin), sv("c.s.t"), sv("id"),
                                           sv("secret"), sv("123")};
    const zerobus_string_view_t empty = {NULL, 0};
    const struct {
        size_t arg; /* The index in valid of the argument replaced. */
        zerobus_string_view_t value;
        const char *message;
    } cases[] = {
        {0, empty, "workspace URL must be non-empty valid UTF-8"},
        {0, sv("127.0.0.1"),
         "workspace URL must start with http:// or https://: 127.0.0.1"},
        {0, sv(with_path), NULL},
        {1, empty, "table name must be non-empty valid UTF-8"},
        {1, sv("c.s"),
         "table name must be fully qualified as catalog.schema.table"},
        {1, sv("c..t"), NULL},
        {1, sv("c.s.t.x"), NULL},
        {2, empty, "client id and secret must be non-empty valid UTF-8"},
        {3, empty, NULL},
        {3, sv("\xff"), NULL},
        {4, empty, "workspace id must be non-empty valid UTF-8"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        zerobus_string_view_t args[5];
        memcpy(args, valid, sizeof(args));
        args[cases[i].arg] = cases[i].value;
        CHECK_EQ_INT(zerobus_get_token(args[0], args[1], args[2], args[3],
                                       args[4], &token, &error),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
        CHECK(error != NULL && !zerobus_error_is_retryable(error));
        CHECK(cases[i].message == NULL ||
              zb_view_equals(zerobus_error_message(error), cases[i].message));
        zerobus_error_free(error);
        error = NULL;
    }
    CHECK_EQ_INT(zerobus_get_token(valid[0], valid[1], valid[2], valid[3],
                                   valid[4], NULL, &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    /* An error left from an earlier call is refused, not overwritten. */
    zerobus_error_t *left = error;
    CHECK_EQ_INT(zerobus_get_token(valid[0], valid[1], valid[2], valid[3],
                                   valid[4], &token, &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(error == left);
    http_server_stop(&server);
    CHECK_EQ_INT(server.served, 0);
    CHECK(token.access_token == NULL && token.expires_at_unix_ms == 7);

zb_cleanup:
    zerobus_error_free(error);
}

int main(void)
{
    test_get_token();
    test_no_expiry();
    test_http_errors();
    test_unreachable();
    test_invalid_arguments();
    TEST_MAIN_RETURN();
}
