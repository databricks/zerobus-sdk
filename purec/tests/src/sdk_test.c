/* Unit tests for the SDK builder and SDK handle (sdk.c). */
#include "test_common.h"

static void test_sdk_builder_validation(void)
{
    zerobus_error_t *err = NULL;

    /* NULL out_builder. */
    CHECK_EQ_INT(zerobus_sdk_builder_new(NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    zerobus_sdk_builder_t *b = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_new(&b, &err), ZEROBUS_STATUS_OK);
    CHECK(b != NULL);
    CHECK(err == NULL);

    /* Empty endpoint views are invalid, for both setters. */
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(
                     b, (zerobus_string_view_t){NULL, 0}, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_set_unity_catalog_endpoint(
                     b, (zerobus_string_view_t){NULL, 0}, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Build with no endpoint at all. */
    zerobus_sdk_t *sdk = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_build(b, &sdk, &err),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK(sdk == NULL);
    zerobus_error_free(err);
    err = NULL;

    /* Endpoint set, but no UC endpoint. */
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(
                     b, sv("https://ws.zerobus.r.cloud.databricks.com"), &err),
                 ZEROBUS_STATUS_OK);
    CHECK_EQ_INT(zerobus_sdk_builder_build(b, &sdk, &err),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    zerobus_error_free(err);
    err = NULL;

    zerobus_sdk_builder_free(b);
}

static void test_sdk_builder_endpoint_rules(void)
{
    zerobus_error_t *err = NULL;
    zerobus_sdk_builder_t *b = NULL;
    zerobus_sdk_builder_new(&b, NULL);

    /* The setter validates the URL immediately (fail-fast), leaving the
     * builder's previous value untouched on rejection. */

    /* Rejected: missing scheme. */
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(
                     b, sv("ws.zerobus.databricks.com"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(err != NULL);
    zerobus_error_free(err);
    err = NULL;

    /* Rejected: no host, an empty label, a malformed port. */
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(b, sv("https://"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(b, sv("https://a..b"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(
                     b, sv("https://ws.zerobus.databricks.com:notaport"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Rejected: a path or a trailing slash — only a bare origin is stored. */
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(
                     b, sv("https://ws.zerobus.databricks.com/path"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(
                     b, sv("https://ws.zerobus.databricks.com/"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Accepted: http (plaintext, for local/dev) and a single-label host — no
     * https-only or workspace-subdomain rule. */
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(
                     b, sv("http://ws.zerobus.databricks.com"), &err),
                 ZEROBUS_STATUS_OK);
    CHECK(err == NULL);
    CHECK_EQ_INT(
        zerobus_sdk_builder_set_endpoint(b, sv("https://localhost:8080"), &err),
        ZEROBUS_STATUS_OK);
    CHECK(err == NULL);

    /* The UC endpoint setter uses the same rules. */
    CHECK_EQ_INT(zerobus_sdk_builder_set_unity_catalog_endpoint(
                     b, sv("https://a..b"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_set_unity_catalog_endpoint(
                     b, sv("http://localhost"), &err),
                 ZEROBUS_STATUS_OK);
    CHECK(err == NULL);

    zerobus_sdk_builder_free(b);
}

/* NULL-builder setters and NULL out_sdk. */
static void test_sdk_builder_edges(void)
{
    zerobus_error_t *err = NULL;

    CHECK_EQ_INT(
        zerobus_sdk_builder_set_endpoint(NULL, sv("https://a.b"), &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_set_unity_catalog_endpoint(
                     NULL, sv("https://a.b"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    zerobus_sdk_builder_t *b = NULL;
    zerobus_sdk_builder_new(&b, NULL);

    /* NULL out_sdk. */
    zerobus_sdk_builder_set_endpoint(
        b, sv("https://ws.zerobus.r.cloud.databricks.com"), NULL);
    zerobus_sdk_builder_set_unity_catalog_endpoint(
        b, sv("https://ws.cloud.databricks.com"), NULL);
    CHECK_EQ_INT(zerobus_sdk_builder_build(b, NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);

    zerobus_sdk_builder_free(b);
}

static void test_sdk_build_ok(void)
{
    /* A valid configuration builds an SDK locally (no network at build time).
     */
    zerobus_sdk_builder_t *b = NULL;
    zerobus_sdk_builder_new(&b, NULL);
    zerobus_sdk_builder_set_endpoint(
        b, sv("https://myws.zerobus.us-west.cloud.databricks.com"), NULL);
    zerobus_sdk_builder_set_unity_catalog_endpoint(
        b, sv("https://myws.cloud.databricks.com"), NULL);

    zerobus_error_t *err = NULL;
    zerobus_sdk_t *sdk = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_build(b, &sdk, &err), ZEROBUS_STATUS_OK);
    CHECK(sdk != NULL);
    CHECK(err == NULL);

    zerobus_sdk_builder_free(b);
    zerobus_sdk_free(sdk);
}

/* A rejected setter is transactional: it leaves the previous value in place. */
static void test_sdk_setter_transactional(void)
{
    zerobus_sdk_builder_t *b = NULL;
    zerobus_sdk_builder_new(&b, NULL);

    CHECK_EQ_INT(
        zerobus_sdk_builder_set_endpoint(
            b, sv("https://myws.zerobus.us-west.cloud.databricks.com"), NULL),
        ZEROBUS_STATUS_OK);
    CHECK_EQ_INT(zerobus_sdk_builder_set_unity_catalog_endpoint(
                     b, sv("https://myws.cloud.databricks.com"), NULL),
                 ZEROBUS_STATUS_OK);

    /* A later rejected set_endpoint must not clobber the good value. */
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(b, sv("https://a..b"), NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);

    /* Build still succeeds using the endpoint set before the failed call. */
    zerobus_sdk_t *sdk = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_build(b, &sdk, NULL), ZEROBUS_STATUS_OK);
    CHECK(sdk != NULL);

    zerobus_sdk_builder_free(b);
    zerobus_sdk_free(sdk);
}

/* Every SDK-side *_free accepts NULL. */
static void test_sdk_free_null_safe(void)
{
    zerobus_sdk_builder_free(NULL);
    zerobus_sdk_free(NULL);
    CHECK(1);
}

/* A non-NULL *out_error on entry is refused at every entry point, and the
 * existing error is left untouched (not overwritten, freed, or replaced). */
static void test_sdk_out_error_must_be_null(void)
{
    zerobus_error_t *err = NULL;
    /* Seed a live error through a genuine failure. */
    CHECK_EQ_INT(zerobus_sdk_builder_new(NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(err != NULL);
    const zerobus_error_t *seeded = err;

    /* The guard runs first, so the other arguments do not matter. */
    zerobus_sdk_builder_t *b = NULL;
    zerobus_sdk_t *sdk = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_new(&b, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(b, sv("https://a.b.c"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_sdk_builder_set_unity_catalog_endpoint(
                     b, sv("https://a.b"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_sdk_builder_build(b, &sdk, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);

    CHECK(err == seeded); /* same object: untouched */
    CHECK(b == NULL);
    CHECK(sdk == NULL);
    zerobus_error_free(err);
}

int main(void)
{
    test_sdk_builder_validation();
    test_sdk_builder_endpoint_rules();
    test_sdk_builder_edges();
    test_sdk_build_ok();
    test_sdk_setter_transactional();
    test_sdk_free_null_safe();
    test_sdk_out_error_must_be_null();
    TEST_MAIN_RETURN();
}
