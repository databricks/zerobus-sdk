/* Unit tests for header sets and headers providers (headers_provider.c). */
#include <string.h>

#include "headers_provider.h"
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
    REQUIRE(headers.count == 6);
    CHECK(strcmp(headers.items[4].name, "x-key.1_a") == 0);
    CHECK(strcmp(headers.items[4].value, "Value 1 !~") == 0);
    CHECK(strcmp(headers.items[5].value, "") == 0);

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
    CHECK_OK(zb_headers_provider_get(provider, &headers));
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

/* A failed get returns the callback's status and discards its headers. */
static void test_provider_get_failure(void)
{
    struct provider_state state = {0};
    zerobus_headers_provider_t *provider = NULL;
    zerobus_headers_t headers = {0};

    REQUIRE_OK(new_counting(add_then_fail, &state, &provider));
    CHECK_EQ_INT(zb_headers_provider_get(provider, &headers),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(state.gets, 1);
    CHECK(headers.count == 0 && headers.items == NULL);

zb_cleanup:
    zb_headers_clear(&headers);
    zerobus_headers_provider_free(provider);
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
        CHECK_OK(zb_headers_provider_get(provider, &headers));
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
    CHECK_OK(zb_headers_provider_get(provider, &headers));
    CHECK(headers.count == 0);

zb_cleanup:
    zb_headers_clear(&headers);
    zerobus_headers_provider_free(provider);
}

/* Not implemented yet: get fails with UNIMPLEMENTED and adds nothing. */
static void test_oauth_provider(void)
{
    zerobus_headers_provider_t *provider = NULL;
    zerobus_headers_t headers = {0};

    REQUIRE_OK(zb_oauth_headers_provider_new("https://ws.cloud.example.com",
                                             "123", "c.s.t", "id", "secret",
                                             &provider));
    CHECK_EQ_INT(zb_headers_provider_get(provider, &headers),
                 ZEROBUS_STATUS_UNIMPLEMENTED);
    CHECK(headers.count == 0);

zb_cleanup:
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
    test_provider_references();
    test_provider_optional_callbacks();
    test_provider_set_invalidate_invalid();
    test_static_provider();
    test_static_provider_invalid();
    test_static_provider_no_headers();
    test_oauth_provider();
    TEST_MAIN_RETURN();
}
