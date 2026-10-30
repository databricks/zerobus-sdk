/* Unit tests for oauth.c, against a Unity Catalog token endpoint played by
 * the scripted HTTP server on a loopback port. */
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "http_server.h"
#include "internal/common/utils.h"
#include "internal/oauth/oauth.h"
#include "loopback.h"
#include "test_common.h"

enum { TIMEOUT_MS = 5000 };

/* The form body for workspace 123 and table c.s.t: the authorization details
 * are the JSON privileges for the catalog, the schema and the table, one line
 * of JSON per line here. */
static const char expected_body[] =
    "scope=all-apis"
    "&resource=api%3A%2F%2Fdatabricks%2Fworkspaces%2F123%2F"
    "zerobusDirectWriteApi"
    "&authorization_details="
    "%5B%0A"
    "%20%20%7B%0A"
    "%20%20%20%20%22type%22%3A%20%22unity_catalog_privileges%22%2C%0A"
    "%20%20%20%20%22privileges%22%3A%20%5B%22USE%20CATALOG%22%5D%2C%0A"
    "%20%20%20%20%22object_type%22%3A%20%22CATALOG%22%2C%0A"
    "%20%20%20%20%22object_full_path%22%3A%20%22c%22%0A"
    "%20%20%7D%2C%0A"
    "%20%20%7B%0A"
    "%20%20%20%20%22type%22%3A%20%22unity_catalog_privileges%22%2C%0A"
    "%20%20%20%20%22privileges%22%3A%20%5B%22USE%20SCHEMA%22%5D%2C%0A"
    "%20%20%20%20%22object_type%22%3A%20%22SCHEMA%22%2C%0A"
    "%20%20%20%20%22object_full_path%22%3A%20%22c.s%22%0A"
    "%20%20%7D%2C%0A"
    "%20%20%7B%0A"
    "%20%20%20%20%22type%22%3A%20%22unity_catalog_privileges%22%2C%0A"
    "%20%20%20%20%22privileges%22%3A%20%5B%22SELECT%22%2C"
    "%20%22MODIFY%22%5D%2C%0A"
    "%20%20%20%20%22object_type%22%3A%20%22TABLE%22%2C%0A"
    "%20%20%20%20%22object_full_path%22%3A%20%22c.s.t%22%2C%0A"
    "%20%20%20%20%22operations%22%3A%20%5B%22zerobuswrite%22%5D%0A"
    "%20%20%7D%0A"
    "%5D"
    "&grant_type=client_credentials";

/* ---- helpers ----------------------------------------------------------- */

/* Fetches a token for client_id, with secret "secret", from server, which
 * answers with status_line and body. Returns the fetch's status, or UNKNOWN
 * if the server did not start. The server has stopped by then, so its fields
 * can be read. */
static zerobus_status_t fetch(http_server_t *server, const char *client_id,
                              const char *status_line, const char *body,
                              zb_fetched_token_t *out_token,
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
    zerobus_status_t status = zb_oauth_fetch_token(
        NULL, origin, "123", "c.s.t", client_id, "secret",
        zb_deadline_after_ms(TIMEOUT_MS), out_token, out_error);
    http_server_stop(server);
    return status;
}

/* Fetches as client "id" from a server that answers 200 with body. */
static zerobus_status_t fetch_body(const char *body,
                                   zb_fetched_token_t *out_token,
                                   zerobus_error_t **out_error)
{
    http_server_t server = {0};
    return fetch(&server, "id", "200 OK", body, out_token, out_error);
}

/* ---- tests ------------------------------------------------------------- */

static void test_fetch_token(void)
{
    http_server_t server = {0};
    zb_fetched_token_t token = {0};

    REQUIRE_OK(fetch(&server, "id", "200 OK",
                     "{\"access_token\":\"tok\",\"expires_in\":3600}", &token,
                     NULL));
    CHECK(strcmp(token.token, "tok") == 0);
    CHECK_EQ_INT(token.expires_in, 3600);
    const char *body = strstr(server.request, "\r\n\r\n");
    CHECK(strncmp(server.request, "POST /oidc/v1/token HTTP/1.1\r\n", 30) == 0);
    CHECK(strstr(server.request, "\r\nauthorization: Basic aWQ6c2VjcmV0\r\n"));
    CHECK(strstr(server.request,
                 "\r\nContent-Type: application/x-www-form-urlencoded\r\n"));
    CHECK(body != NULL && strcmp(body + 4, expected_body) == 0);

zb_cleanup:
    zb_fetched_token_clear(&token);
    CHECK(token.token == NULL && token.expires_in == 0);
}

/* The credentials are base64 encoded, with padding. */
static void test_authorization(void)
{
    static const struct {
        const char *client_id;
        const char *header;
    } cases[] = {
        {"a", "\r\nauthorization: Basic YTpzZWNyZXQ=\r\n"},
        {"abc", "\r\nauthorization: Basic YWJjOnNlY3JldA==\r\n"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        http_server_t server = {0};
        zb_fetched_token_t token = {0};
        CHECK_OK(fetch(&server, cases[i].client_id, "200 OK",
                       "{\"access_token\":\"t\"}", &token, NULL));
        CHECK(strstr(server.request, cases[i].header) != NULL);
        zb_fetched_token_clear(&token);
    }
}

/* expires_in may be a string of digits; anything else that is not a positive
 * number gives 0. */
static void test_expires_in(void)
{
    static const struct {
        const char *expires_in;
        uint64_t seconds;
    } cases[] = {
        {"\" 3600 \"", 3600},
        {"0", 0},
        {"-5", 0},
        {"3600.5", 0},
        {"\"12x\"", 0},
        {"\"\"", 0},
        {"\"99999999999999999999\"", 0},
        {"null", 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        char body[128];
        snprintf(body, sizeof(body),
                 "{\"access_token\":\"t\",\"expires_in\":%s}",
                 cases[i].expires_in);
        zb_fetched_token_t token = {0};
        CHECK_OK(fetch_body(body, &token, NULL));
        CHECK_EQ_INT(token.expires_in, cases[i].seconds);
        zb_fetched_token_clear(&token);
    }
    zb_fetched_token_t token = {0};
    CHECK_OK(fetch_body("{\"access_token\":\"t\"}", &token, NULL));
    CHECK_EQ_INT(token.expires_in, 0);
    zb_fetched_token_clear(&token);
}

/* A 2xx body without an access_token that is a non-empty header value is
 * rejected, leaving the token untouched. */
static void test_invalid_response(void)
{
    static const char *const bodies[] = {
        "not json",
        "[\"access_token\"]",
        "{}",
        "{\"access_token\":7}",
        "{\"access_token\":\"\"}",
        "{\"access_token\":\"a\\u0001b\"}", /* a control byte */
    };
    zb_fetched_token_t token = {NULL, 7};
    for (size_t i = 0; i < sizeof(bodies) / sizeof(bodies[0]); ++i) {
        zerobus_error_t *error = NULL;
        CHECK_EQ_INT(fetch_body(bodies[i], &token, &error),
                     ZEROBUS_STATUS_INTERNAL);
        CHECK(error != NULL);
        zerobus_error_free(error);
    }
    CHECK(token.token == NULL && token.expires_in == 7);
}

/* A 5xx response may succeed on retry, and any other non-2xx one will not.
 * The error quotes a short text body. */
static void test_http_errors(void)
{
    static const struct {
        const char *status_line;
        const char *body;
        zerobus_status_t status;
        const char *message;
    } cases[] = {
        {"503 Service Unavailable", "", ZEROBUS_STATUS_UNAVAILABLE,
         "token request failed with HTTP status 503"},
        {"401 Unauthorized", "{\"error\":\"invalid_client\"}",
         ZEROBUS_STATUS_UNAUTHENTICATED,
         "token request failed with HTTP status 401: "
         "{\"error\":\"invalid_client\"}"},
        {"302 Found", "\xff", ZEROBUS_STATUS_UNAUTHENTICATED,
         "token request failed with HTTP status 302"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        http_server_t server = {0};
        zb_fetched_token_t token = {NULL, 7};
        zerobus_error_t *error = NULL;
        CHECK_EQ_INT(fetch(&server, "id", cases[i].status_line, cases[i].body,
                           &token, &error),
                     cases[i].status);
        CHECK(error != NULL &&
              zb_view_equals(zerobus_error_message(error), cases[i].message));
        CHECK(token.token == NULL && token.expires_in == 7);
        zerobus_error_free(error);
    }
}

/* A request that fails returns the HTTP client's status. */
static void test_unreachable(void)
{
    zb_fetched_token_t token = {NULL, 7};
    zerobus_error_t *error = NULL;
    uint16_t port = 0;
    int listener = loopback_listen(&port);
    REQUIRE(listener >= 0);
    close(listener);
    char origin[32];
    snprintf(origin, sizeof(origin), "http://127.0.0.1:%u", (unsigned)port);

    CHECK_EQ_INT(
        zb_oauth_fetch_token(NULL, origin, "123", "c.s.t", "id", "secret",
                             zb_deadline_after_ms(TIMEOUT_MS), &token, &error),
        ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(error != NULL);
    CHECK(token.token == NULL && token.expires_in == 7);

zb_cleanup:
    zerobus_error_free(error);
}

int main(void)
{
    test_fetch_token();
    test_authorization();
    test_expires_in();
    test_invalid_response();
    test_http_errors();
    test_unreachable();
    TEST_MAIN_RETURN();
}
