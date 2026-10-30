/* Unit tests for header sets and headers providers (headers_provider.c). The
 * OAuth provider fetches from a Unity Catalog played by the scripted HTTP
 * server on a loopback port. */
#define _GNU_SOURCE

#include <stdio.h>
#include <string.h>

#include "headers_provider.h"
#include "http_server.h"
#include "sdk.h"
#include "stream.h"
#include "test_common.h"

/* ---- helpers ----------------------------------------------------------- */

/* Counts the calls a provider makes to its callbacks. */
struct provider_state {
    int gets;
    int invalidations;
    int destroys;
};

static zerobus_status_t add_authorization(void *user_data,
                                          zerobus_headers_t *headers)
{
    ((struct provider_state *)user_data)->gets++;
    return zerobus_headers_add(headers, sv("authorization"), sv("Bearer token"),
                               NULL);
}

/* Adds a header, then fails. */
static zerobus_status_t add_then_fail(void *user_data,
                                      zerobus_headers_t *headers)
{
    add_authorization(user_data, headers);
    return ZEROBUS_STATUS_UNAVAILABLE;
}

/* Fails with a message, after replacing an earlier one. */
static zerobus_status_t fail_with_message(void *user_data,
                                          zerobus_headers_t *headers)
{
    (void)user_data;
    zerobus_headers_set_error_message(headers, sv("first"), NULL);
    zerobus_headers_set_error_message(headers, sv("401 invalid_client"), NULL);
    return ZEROBUS_STATUS_UNAUTHENTICATED;
}

static bool has_message(const zerobus_error_t *error, const char *text)
{
    zerobus_string_view_t message = zerobus_error_message(error);
    return message.len == strlen(text) &&
           memcmp(message.data, text, message.len) == 0;
}

static void count_invalidation(void *user_data)
{
    ((struct provider_state *)user_data)->invalidations++;
}

static void count_destroy(void *user_data)
{
    ((struct provider_state *)user_data)->destroys++;
}

/* Creates a provider that counts its callbacks in *state. */
static zerobus_status_t new_counting(zerobus_get_headers_fn get_headers,
                                     struct provider_state *state,
                                     zerobus_headers_provider_t **out_provider)
{
    zerobus_status_t status = zerobus_headers_provider_new(
        get_headers, count_destroy, state, out_provider, NULL);
    if (status == ZEROBUS_STATUS_OK) {
        status = zerobus_headers_provider_set_invalidate(
            *out_provider, count_invalidation, NULL);
    }
    return status;
}

/* A stream whose SDK points at a Unity Catalog played by server. */
typedef struct {
    http_server_t server;
    bool started;
    char reply[256];
    char origin[32];
    struct zerobus_sdk sdk;
    struct zerobus_stream stream;
} oauth_fixture_t;

/* Starts the server, which answers every token request with status_line and
 * body, and sets up the stream for table c.s.t with the default timeouts. */
static bool oauth_fixture_start(oauth_fixture_t *f, const char *status_line,
                                const char *body)
{
    snprintf(f->reply, sizeof(f->reply),
             "HTTP/1.1 %s\r\nContent-Length: %zu\r\n\r\n%s", status_line,
             strlen(body), body);
    f->server.reply[0] = f->reply;
    f->started = http_server_start(&f->server);
    snprintf(f->origin, sizeof(f->origin), "http://127.0.0.1:%u",
             (unsigned)f->server.port);
    f->sdk = (struct zerobus_sdk){.uc_endpoint = f->origin,
                                  .workspace_id = (char *)"123"};
    f->stream = (struct zerobus_stream){.sdk = &f->sdk,
                                        .table = (char *)"c.s.t",
                                        .config = zb_stream_config_default};
    return f->started;
}

/* Stops the server, if it runs, so its fields can be read. */
static void oauth_fixture_stop(oauth_fixture_t *f)
{
    if (f->started) {
        f->started = false;
        http_server_stop(&f->server);
    }
}

/* ---- tests ------------------------------------------------------------- */

static void test_add(void)
{
    zerobus_headers_t headers = {0};
    for (int i = 0; i < 5; ++i) {
        CHECK_OK(zerobus_headers_add(&headers, sv("x-key.1_a"),
                                     sv("Value 1 !~"), NULL));
    }
    CHECK_OK(zerobus_headers_add(&headers, sv("empty"),
                                 (zerobus_string_view_t){NULL, 0}, NULL));
    /* Names are stored lowercase; values are kept as given. */
    CHECK_OK(
        zerobus_headers_add(&headers, sv("X-Mixed.Case"), sv("Mixed"), NULL));
    /* The SDK sets the table name and user agent itself, so they're ignored. */
    CHECK_OK(zerobus_headers_add(
        &headers, sv("X-Databricks-Zerobus-Table-Name"), sv("c.s.t"), NULL));
    CHECK_OK(
        zerobus_headers_add(&headers, sv("user-agent"), sv("app/1.0"), NULL));
    REQUIRE(headers.count == 7);
    CHECK(strcmp(headers.items[4].name, "x-key.1_a") == 0);
    CHECK(strcmp(headers.items[4].value, "Value 1 !~") == 0);
    CHECK(strcmp(headers.items[5].value, "") == 0);
    CHECK(strcmp(headers.items[6].name, "x-mixed.case") == 0);
    CHECK(strcmp(headers.items[6].value, "Mixed") == 0);

zb_cleanup:
    zb_headers_clear(&headers);
    CHECK(headers.count == 0 && headers.items == NULL);
}

static void test_add_invalid(void)
{
    zerobus_headers_t headers = {0};
    zerobus_error_t *error = NULL;
    CHECK_EQ_INT(zerobus_headers_add(&headers, (zerobus_string_view_t){NULL, 0},
                                     sv("v"), &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(error != NULL);
    zerobus_error_free(error);
    error = NULL;
    CHECK_EQ_INT(zerobus_headers_add(&headers, sv("a"),
                                     (zerobus_string_view_t){NULL, 1}, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    /* Names are gRPC metadata keys the transport doesn't own, in any case,
     * and values are printable ASCII. */
    const zerobus_string_view_t bad_names[] = {sv("grpc-status"),
                                               sv("Grpc-Status"),
                                               sv(":path"),
                                               sv("TE"),
                                               {"a\0b", 3}};
    for (size_t i = 0; i < sizeof(bad_names) / sizeof(bad_names[0]); ++i) {
        CHECK_EQ_INT(zerobus_headers_add(&headers, bad_names[i], sv("v"), NULL),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
    }
    CHECK_EQ_INT(zerobus_headers_add(&headers, sv("authorization"),
                                     sv("caf\xc3\xa9"), NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_headers_add_internal(&headers, NULL, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_headers_add(NULL, sv("a"), sv("b"), &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    /* An error left from an earlier call is refused, not overwritten. */
    zerobus_error_t *left = error;
    CHECK_EQ_INT(zerobus_headers_add(&headers, sv("a"), sv("b"), &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(error == left);
    CHECK(headers.count == 0);
    zerobus_error_free(error);
    zb_headers_clear(&headers);
}

/* A rejected provider leaves out_provider and user_data untouched. */
static void test_provider_new_invalid(void)
{
    struct provider_state state = {0};
    int marker;
    zerobus_headers_provider_t *const sentinel =
        (zerobus_headers_provider_t *)&marker;
    zerobus_headers_provider_t *provider = sentinel;
    zerobus_error_t *error = NULL;

    CHECK_EQ_INT(zerobus_headers_provider_new(NULL, count_destroy, &state,
                                              &provider, &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(error != NULL);
    zerobus_error_free(error);
    error = NULL;
    CHECK_EQ_INT(zerobus_headers_provider_new(add_authorization, count_destroy,
                                              &state, NULL, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);

    /* An error left from an earlier call is refused, not overwritten. */
    CHECK_EQ_INT(zerobus_headers_provider_new(NULL, count_destroy, &state,
                                              &provider, &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_t *left = error;
    CHECK_EQ_INT(zerobus_headers_provider_new(add_authorization, count_destroy,
                                              &state, &provider, &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(error == left);
    zerobus_error_free(error);

    CHECK(provider == sentinel);
    CHECK_EQ_INT(state.destroys, 0);
}

/* get and invalidate call the provider's callbacks with its user_data. */
static void test_provider_callbacks(void)
{
    struct provider_state state = {0};
    zerobus_headers_provider_t *provider = NULL;
    zerobus_headers_t headers = {0};

    REQUIRE_OK(new_counting(add_authorization, &state, &provider));
    CHECK_OK(zb_headers_provider_get(provider, &headers, NULL));
    CHECK_EQ_INT(state.gets, 1);
    REQUIRE(headers.count == 1);
    CHECK(strcmp(headers.items[0].name, "authorization") == 0);
    CHECK(strcmp(headers.items[0].value, "Bearer token") == 0);
    zb_headers_provider_invalidate(provider);
    CHECK_EQ_INT(state.invalidations, 1);

zb_cleanup:
    zb_headers_clear(&headers);
    zerobus_headers_provider_free(provider);
    CHECK_EQ_INT(state.destroys, 1);
}

/* A failed get returns the callback's status, discards its headers, and
 * reports the callback's message if it set one. */
static void test_provider_get_failure(void)
{
    struct provider_state state = {0};
    zerobus_headers_provider_t *provider = NULL;
    zerobus_headers_t headers = {0};
    zerobus_error_t *error = NULL;

    REQUIRE_OK(new_counting(add_then_fail, &state, &provider));
    CHECK_EQ_INT(zb_headers_provider_get(provider, &headers, &error),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(state.gets, 1);
    CHECK(headers.count == 0 && headers.items == NULL);
    CHECK(has_message(error, "headers provider failed"));
    zerobus_error_free(error);
    error = NULL;
    zerobus_headers_provider_free(provider);
    provider = NULL;

    REQUIRE_OK(zerobus_headers_provider_new(fail_with_message, NULL, NULL,
                                            &provider, NULL));
    CHECK_EQ_INT(zb_headers_provider_get(provider, &headers, &error),
                 ZEROBUS_STATUS_UNAUTHENTICATED);
    CHECK_EQ_INT(zerobus_error_status(error), ZEROBUS_STATUS_UNAUTHENTICATED);
    CHECK(has_message(error, "headers provider failed: 401 invalid_client"));
    CHECK(headers.error_message == NULL);

zb_cleanup:
    zerobus_error_free(error);
    zb_headers_clear(&headers);
    zerobus_headers_provider_free(provider);
}

/* An error message must be non-empty valid UTF-8, and is ignored on OK. */
static void test_set_error_message(void)
{
    zerobus_headers_t headers = {0};
    zerobus_error_t *error = NULL;

    CHECK_EQ_INT(zerobus_headers_set_error_message(NULL, sv("m"), &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(error != NULL);
    zerobus_error_free(error);
    error = NULL;
    CHECK_EQ_INT(zerobus_headers_set_error_message(
                     &headers, (zerobus_string_view_t){NULL, 0}, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zerobus_headers_set_error_message(&headers, sv("\xff"), &error),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    /* An error left from an earlier call is refused, not overwritten. */
    zerobus_error_t *left = error;
    CHECK_EQ_INT(zerobus_headers_set_error_message(&headers, sv("m"), &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(error == left);
    zerobus_error_free(error);
    CHECK(headers.error_message == NULL);
}

/* destroy runs once, when the last reference is released. */
static void test_provider_references(void)
{
    struct provider_state state = {0};
    zerobus_headers_provider_t *provider = NULL;

    REQUIRE_OK(new_counting(add_authorization, &state, &provider));
    zb_headers_provider_ref(provider);
    zb_headers_provider_ref(provider);
    zerobus_headers_provider_free(provider);
    zerobus_headers_provider_free(provider);
    CHECK_EQ_INT(state.destroys, 0);

zb_cleanup:
    zerobus_headers_provider_free(provider);
    CHECK_EQ_INT(state.destroys, 1);
    zerobus_headers_provider_free(NULL);
}

/* invalidate and destroy are optional. */
static void test_provider_optional_callbacks(void)
{
    struct provider_state state = {0};
    zerobus_headers_provider_t *provider = NULL;

    REQUIRE_OK(zerobus_headers_provider_new(add_authorization, NULL, &state,
                                            &provider, NULL));
    zb_headers_provider_invalidate(provider);
    CHECK_EQ_INT(state.invalidations, 0);

    /* Setting NULL removes an invalidate callback. */
    CHECK_OK(zerobus_headers_provider_set_invalidate(provider,
                                                     count_invalidation, NULL));
    CHECK_OK(zerobus_headers_provider_set_invalidate(provider, NULL, NULL));
    zb_headers_provider_invalidate(provider);
    CHECK_EQ_INT(state.invalidations, 0);

zb_cleanup:
    zerobus_headers_provider_free(provider);
    CHECK_EQ_INT(state.destroys, 0);
}

/* invalidate can't be set on a NULL provider, or once another reference is
 * held. */
static void test_provider_set_invalidate_invalid(void)
{
    struct provider_state state = {0};
    zerobus_headers_provider_t *provider = NULL;
    zerobus_error_t *error = NULL;

    CHECK_EQ_INT(zerobus_headers_provider_set_invalidate(
                     NULL, count_invalidation, &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(error != NULL);
    zerobus_error_free(error);
    error = NULL;

    REQUIRE_OK(zerobus_headers_provider_new(add_authorization, NULL, &state,
                                            &provider, NULL));
    zb_headers_provider_ref(provider);
    CHECK_EQ_INT(zerobus_headers_provider_set_invalidate(
                     provider, count_invalidation, &error),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK(error != NULL);
    zerobus_headers_provider_free(provider);

    /* An error left from an earlier call is refused, not overwritten. */
    zerobus_error_t *left = error;
    CHECK_EQ_INT(zerobus_headers_provider_set_invalidate(
                     provider, count_invalidation, &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(error == left);
    zerobus_error_free(error);

    zb_headers_provider_invalidate(provider);
    CHECK_EQ_INT(state.invalidations, 0);

zb_cleanup:
    zerobus_headers_provider_free(provider);
}

/* A static provider sends its own copy of the headers on every get. */
static void test_static_provider(void)
{
    char value[] = "Bearer token";
    const zerobus_string_view_t names[] = {sv("authorization"), sv("x-empty")};
    const zerobus_string_view_t values[] = {sv(value), {NULL, 0}};
    zerobus_headers_provider_t *provider = NULL;
    zerobus_headers_t headers = {0};

    REQUIRE_OK(
        zerobus_static_headers_provider_new(names, values, 2, &provider, NULL));
    value[0] = 'X';
    for (int i = 0; i < 2; ++i) {
        CHECK_OK(zb_headers_provider_get(provider, &headers, NULL));
        REQUIRE(headers.count == 2);
        CHECK(strcmp(headers.items[0].name, "authorization") == 0);
        CHECK(strcmp(headers.items[0].value, "Bearer token") == 0);
        CHECK(strcmp(headers.items[1].name, "x-empty") == 0);
        CHECK(strcmp(headers.items[1].value, "") == 0);
        zb_headers_clear(&headers);
    }
    zb_headers_provider_invalidate(provider);

zb_cleanup:
    zb_headers_clear(&headers);
    zerobus_headers_provider_free(provider);
}

/* Invalid arguments are rejected, and out_provider is left untouched. */
static void test_static_provider_invalid(void)
{
    const zerobus_string_view_t names[] = {sv("a"), {NULL, 0}};
    const zerobus_string_view_t values[] = {sv("b"), sv("v")};
    int marker;
    zerobus_headers_provider_t *const sentinel =
        (zerobus_headers_provider_t *)&marker;
    zerobus_headers_provider_t *provider = sentinel;
    zerobus_error_t *error = NULL;

    /* The second name is empty: the first header is copied, then dropped. */
    CHECK_EQ_INT(zerobus_static_headers_provider_new(names, values, 2,
                                                     &provider, &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(error != NULL);
    zerobus_error_free(error);
    error = NULL;
    CHECK_EQ_INT(
        zerobus_static_headers_provider_new(NULL, values, 1, &provider, NULL),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zerobus_static_headers_provider_new(names, NULL, 1, &provider, NULL),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zerobus_static_headers_provider_new(names, values, 1, NULL, &error),
        ZEROBUS_STATUS_INVALID_ARGUMENT);

    /* An error left from an earlier call is refused, not overwritten. */
    zerobus_error_t *left = error;
    CHECK_EQ_INT(zerobus_static_headers_provider_new(names, values, 1,
                                                     &provider, &error),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(error == left);
    zerobus_error_free(error);

    CHECK(provider == sentinel);
}

/* A static provider with no headers: the opt-out for a table that needs no
 * authentication. */
static void test_static_provider_no_headers(void)
{
    zerobus_headers_provider_t *provider = NULL;
    zerobus_headers_t headers = {0};

    REQUIRE_OK(
        zerobus_static_headers_provider_new(NULL, NULL, 0, &provider, NULL));
    CHECK_OK(zb_headers_provider_get(provider, &headers, NULL));
    CHECK(headers.count == 0);

zb_cleanup:
    zb_headers_clear(&headers);
    zerobus_headers_provider_free(provider);
}

/* Each get adds the authorization header with a token fetched for it. */
static void test_oauth_provider(void)
{
    oauth_fixture_t f = {0};
    zerobus_headers_provider_t *provider = NULL;
    zerobus_headers_t headers = {0};
    REQUIRE(oauth_fixture_start(
        &f, "200 OK", "{\"access_token\":\"tok\",\"expires_in\":3600}"));
    REQUIRE_OK(
        zb_oauth_headers_provider_new(&f.stream, "id", "secret", &provider));

    CHECK_OK(zb_headers_provider_get(provider, &headers, NULL));
    CHECK(headers.count == 1 &&
          strcmp(headers.items[0].name, "authorization") == 0 &&
          strcmp(headers.items[0].value, "Bearer tok") == 0);
    zb_headers_clear(&headers);
    CHECK_OK(zb_headers_provider_get(provider, &headers, NULL));
    oauth_fixture_stop(&f);
    /* Each get fetched a token of its own. */
    CHECK_EQ_INT(f.server.served, 2);

zb_cleanup:
    oauth_fixture_stop(&f);
    zb_headers_clear(&headers);
    zerobus_headers_provider_free(provider);
}

/* A failed fetch fails get, which adds nothing. */
static void test_oauth_provider_failure(void)
{
    oauth_fixture_t f = {0};
    zerobus_headers_provider_t *provider = NULL;
    zerobus_headers_t headers = {0};
    REQUIRE(oauth_fixture_start(&f, "503 Service Unavailable", ""));
    REQUIRE_OK(
        zb_oauth_headers_provider_new(&f.stream, "id", "secret", &provider));

    CHECK_EQ_INT(zb_headers_provider_get(provider, &headers, NULL),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(headers.count == 0);

zb_cleanup:
    oauth_fixture_stop(&f);
    zb_headers_clear(&headers);
    zerobus_headers_provider_free(provider);
}

int main(void)
{
    test_add();
    test_add_invalid();
    test_provider_new_invalid();
    test_provider_callbacks();
    test_provider_get_failure();
    test_set_error_message();
    test_provider_references();
    test_provider_optional_callbacks();
    test_provider_set_invalidate_invalid();
    test_static_provider();
    test_static_provider_invalid();
    test_static_provider_no_headers();
    test_oauth_provider();
    test_oauth_provider_failure();
    TEST_MAIN_RETURN();
}
