/* Unit tests for the SDK builder and SDK handle (sdk.c). */
#include "sdk.h"
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
    CHECK_OK(zerobus_sdk_builder_new(&b, &err));
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
    CHECK_OK(zerobus_sdk_builder_set_endpoint(
        b, sv("https://ws.zerobus.r.cloud.databricks.com"), &err));
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
    CHECK_OK(zerobus_sdk_builder_set_endpoint(
        b, sv("http://ws.zerobus.databricks.com"), &err));
    CHECK(err == NULL);
    CHECK_OK(zerobus_sdk_builder_set_endpoint(b, sv("https://localhost:8080"),
                                              &err));
    CHECK(err == NULL);

    /* The UC endpoint setter uses the same rules. */
    CHECK_EQ_INT(zerobus_sdk_builder_set_unity_catalog_endpoint(
                     b, sv("https://a..b"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_OK(zerobus_sdk_builder_set_unity_catalog_endpoint(
        b, sv("http://localhost"), &err));
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
    CHECK_OK(zerobus_sdk_builder_build(b, &sdk, &err));
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

    CHECK_OK(zerobus_sdk_builder_set_endpoint(
        b, sv("https://myws.zerobus.us-west.cloud.databricks.com"), NULL));
    CHECK_OK(zerobus_sdk_builder_set_unity_catalog_endpoint(
        b, sv("https://myws.cloud.databricks.com"), NULL));

    /* A later rejected set_endpoint must not clobber the good value. */
    CHECK_EQ_INT(zerobus_sdk_builder_set_endpoint(b, sv("https://a..b"), NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);

    /* Build still succeeds using the endpoint set before the failed call. */
    zerobus_sdk_t *sdk = NULL;
    CHECK_OK(zerobus_sdk_builder_build(b, &sdk, NULL));
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

static void test_sdk_retained_reference(void)
{
    zerobus_sdk_builder_t *builder = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_sdk_t *retained = NULL;
    zerobus_stream_builder_t *stream_builder = NULL;

    REQUIRE_OK(zerobus_sdk_builder_new(&builder, NULL));
    REQUIRE_OK(
        zerobus_sdk_builder_set_endpoint(builder, sv("https://a.b"), NULL));
    REQUIRE_OK(zerobus_sdk_builder_set_unity_catalog_endpoint(
        builder, sv("https://c.d"), NULL));
    REQUIRE_OK(zerobus_sdk_builder_build(builder, &sdk, NULL));

    zb_sdk_ref(sdk);
    retained = sdk;
    zerobus_sdk_free(sdk);
    sdk = NULL;
    REQUIRE_OK(zerobus_stream_builder_new(retained, &stream_builder, NULL));
    zerobus_sdk_free(retained);
    retained = NULL;
    CHECK_OK(
        zerobus_stream_builder_set_table(stream_builder, sv("c.s.t"), NULL));

zb_cleanup:
    zerobus_stream_builder_free(stream_builder);
    zerobus_sdk_free(retained);
    zerobus_sdk_free(sdk);
    zerobus_sdk_builder_free(builder);
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

/* The realloc-style out-handle contract: a failed call leaves the caller's
 * out-handle untouched — the handle is written only on success. */
static void test_sdk_out_handle_untouched_on_failure(void)
{
    /* A sentinel we never dereference: any non-NULL value distinct from a real
     * handle works, so point it at a local object. */
    int marker;
    zerobus_sdk_builder_t *const builder_sentinel =
        (zerobus_sdk_builder_t *)&marker;
    zerobus_sdk_t *const sdk_sentinel = (zerobus_sdk_t *)&marker;

    /* Seed a live error through a genuine failure. */
    zerobus_error_t *err = NULL;
    CHECK_EQ_INT(zerobus_sdk_builder_new(NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(err != NULL);

    /* builder_new rejected for a NULL sdk: out_builder is not touched. */
    zerobus_sdk_builder_t *b = builder_sentinel;
    CHECK_EQ_INT(zerobus_sdk_builder_new(&b, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(b == builder_sentinel);
    zerobus_error_free(err);
    err = NULL;

    /* A real builder with no endpoints set: build fails the precondition and
     * must not touch out_sdk. */
    b = NULL;
    REQUIRE_OK(zerobus_sdk_builder_new(&b, NULL));
    zerobus_sdk_t *sdk = sdk_sentinel;
    CHECK_EQ_INT(zerobus_sdk_builder_build(b, &sdk, &err),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK(sdk == sdk_sentinel);
    zerobus_error_free(err);
    err = NULL;

    /* build rejected for a NULL builder: out_sdk still untouched. */
    sdk = sdk_sentinel;
    CHECK_EQ_INT(zerobus_sdk_builder_build(NULL, &sdk, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(sdk == sdk_sentinel);

zb_cleanup:
    zerobus_error_free(err);
    zerobus_sdk_builder_free(b);
}

int main(void)
{
    test_sdk_builder_validation();
    test_sdk_builder_endpoint_rules();
    test_sdk_builder_edges();
    test_sdk_build_ok();
    test_sdk_setter_transactional();
    test_sdk_free_null_safe();
    test_sdk_retained_reference();
    test_sdk_out_error_must_be_null();
    test_sdk_out_handle_untouched_on_failure();
    TEST_MAIN_RETURN();
}
