/* Unit tests for the stream builder and stream handle (stream.c), against
 * an in-process Zerobus service. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fake_zerobus.h"
#include "internal/platform/concurrency.h"
#include "test_common.h"

/* ---- helpers ----------------------------------------------------------- */

/* The service with default options that most tests use, started by main. */
static fake_zerobus_t *server;

/* Build an SDK that connects to service. Caller frees it. */
static zerobus_sdk_t *make_sdk_for(const fake_zerobus_t *service)
{
    char endpoint[64], uc_endpoint[64];
    snprintf(endpoint, sizeof(endpoint), "http://127.0.0.1:%u",
             (unsigned)fake_zerobus_port(service));
    snprintf(uc_endpoint, sizeof(uc_endpoint), "http://127.0.0.1:%u",
             (unsigned)fake_zerobus_token_port(service));
    zerobus_sdk_builder_t *b = NULL;
    zerobus_sdk_builder_new(&b, NULL);
    zerobus_sdk_builder_set_endpoint(b, sv(endpoint), NULL);
    zerobus_sdk_builder_set_unity_catalog_endpoint(b, sv(uc_endpoint), NULL);
    zerobus_sdk_t *sdk = NULL;
    zerobus_sdk_builder_build(b, &sdk, NULL);
    zerobus_sdk_builder_free(b);
    return sdk;
}

/* Build a valid SDK for the stream tests. Caller frees it. */
static zerobus_sdk_t *make_sdk(void)
{
    return make_sdk_for(server);
}

/* Build a valid stream for the ingest/flush/close tests. Caller frees the
 * stream and the SDK passed in. */
static zerobus_stream_t *make_stream(zerobus_sdk_t *sdk)
{
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_builder_new(sdk, &stb, NULL);
    zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL);
    zerobus_stream_builder_set_oauth(stb, sv("client-id"), sv("client-secret"),
                                     NULL);
    zerobus_stream_t *stream = NULL;
    zerobus_stream_builder_build(stb, &stream, NULL);
    zerobus_stream_builder_free(stb);
    return stream;
}

static zerobus_status_t no_headers(void *user_data, zerobus_headers_t *headers)
{
    (void)user_data;
    (void)headers;
    return ZEROBUS_STATUS_OK;
}

static void count_destroy(void *user_data)
{
    ++*(int *)user_data;
}

/* Create a provider that counts its destroy calls in *destroys. */
static zerobus_headers_provider_t *make_provider(int *destroys)
{
    zerobus_headers_provider_t *provider = NULL;
    zerobus_headers_provider_new(no_headers, count_destroy, destroys, &provider,
                                 NULL);
    return provider;
}

/* Each build opens a connection, so the build loops run fewer times. */
enum { WORKERS = 5, ITERATIONS = 1000, BUILDS = 20 };

/* Hold workers until setup is complete or cleanup releases them. */
struct start_gate {
    zb_mutex_t mutex;
    zb_cond_t changed;
    bool mutex_initialized;
    bool changed_initialized;
    bool open;
};

/* Synchronization failure must not leave workers using freed test state. */
static void must_sync(zerobus_status_t status)
{
    if (status != ZEROBUS_STATUS_OK) {
        fprintf(stderr, "Worker synchronization failed (status %u).\n",
                (unsigned int)status);
        abort();
    }
}

/* Wait until the test opens the gate, allowing this worker to proceed. */
static void await_start(struct start_gate *gate)
{
    must_sync(zb_mutex_lock(&gate->mutex));
    while (!gate->open) {
        must_sync(zb_cond_wait(&gate->changed, &gate->mutex));
    }
    must_sync(zb_mutex_unlock(&gate->mutex));
}

/* Release and join all started workers, including partially completed setup. */
static void finish_workers(struct start_gate *gate, zb_thread_t *threads,
                           unsigned int created)
{
    if (gate->changed_initialized) {
        must_sync(zb_mutex_lock(&gate->mutex));
        gate->open = true;
        must_sync(zb_cond_broadcast(&gate->changed));
        must_sync(zb_mutex_unlock(&gate->mutex));
    }
    for (unsigned int i = 0; i < created; i++) {
        must_sync(zb_thread_join(&threads[i], NULL));
    }
    if (gate->changed_initialized) {
        CHECK_OK(zb_cond_destroy(&gate->changed));
    }
    if (gate->mutex_initialized) {
        CHECK_OK(zb_mutex_destroy(&gate->mutex));
    }
}

/* Each worker owns a separate builder retaining the same SDK. */
struct ownership_context {
    struct start_gate *gate;
    zerobus_stream_builder_t *builder;
    zerobus_status_t status;
    unsigned int completed;
};

/* Acquire and release stream references while other workers do the same. */
static void *build_and_free_streams(void *arg)
{
    struct ownership_context *context = (struct ownership_context *)arg;
    await_start(context->gate);
    for (unsigned int i = 0; i < BUILDS; i++) {
        zerobus_stream_t *stream = NULL;
        context->status =
            zerobus_stream_builder_build(context->builder, &stream, NULL);
        if (context->status != ZEROBUS_STATUS_OK) {
            break;
        }
        zerobus_stream_free(stream);
        context->completed++;
    }
    zerobus_stream_builder_free(context->builder);
    context->builder = NULL;
    return NULL;
}

/* Give concurrent stream callers independent output and result storage. */
struct stream_context {
    struct start_gate *gate;
    zerobus_stream_t *stream;
    enum { CALL_INGEST, CALL_FLUSH, CALL_CLOSE } operation;
    unsigned int completed;
    zerobus_status_t unexpected_status;
};

/* Accept either side of a racing close, and require every close to succeed. */
static void *call_stream(void *arg)
{
    struct stream_context *context = (struct stream_context *)arg;
    await_start(context->gate);
    for (unsigned int i = 0; i < ITERATIONS; i++) {
        zerobus_error_t *err = NULL;
        zerobus_offset_t offset = -1;
        zerobus_status_t status;
        if (context->operation == CALL_INGEST) {
            status = zerobus_stream_ingest_json_record(context->stream,
                                                       sv("{}"), &offset, &err);
        } else if (context->operation == CALL_FLUSH) {
            status = zerobus_stream_flush(context->stream, &err);
        } else {
            status = zerobus_stream_close(context->stream, &err);
        }
        zerobus_error_free(err);
        bool expected = context->operation == CALL_CLOSE
                            ? status == ZEROBUS_STATUS_OK
                            : status == ZEROBUS_STATUS_OK ||
                                  status == ZEROBUS_STATUS_FAILED_PRECONDITION;
        if (!expected) {
            context->unexpected_status = status;
            break;
        }
        context->completed++;
    }
    return NULL;
}

/* ---- tests ------------------------------------------------------------- */

static void test_stream_builder_validation(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    CHECK(sdk != NULL);

    zerobus_error_t *err = NULL;

    /* NULL sdk. */
    zerobus_stream_builder_t *stb = NULL;
    CHECK_EQ_INT(zerobus_stream_builder_new(NULL, &stb, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    CHECK_OK(zerobus_stream_builder_new(sdk, &stb, &err));
    CHECK(stb != NULL);

    /* Empty table. */
    CHECK_EQ_INT(zerobus_stream_builder_set_table(
                     stb, (zerobus_string_view_t){NULL, 0}, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Not fully qualified (catalog.schema.table). */
    CHECK_EQ_INT(
        zerobus_stream_builder_set_table(stb, sv("schema.table"), &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Empty credentials. */
    CHECK_EQ_INT(
        zerobus_stream_builder_set_oauth(stb, sv(""), sv("secret"), &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Build without a table set: fails before any network work. */
    zerobus_stream_t *stream = NULL;
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &stream, &err),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK(stream == NULL);
    zerobus_error_free(err);
    err = NULL;

    /* Table set but no credentials: fails before any network work. */
    zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL);
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &stream, &err),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK(stream == NULL);
    zerobus_error_free(err);
    err = NULL;

    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
}

/* NULL-builder setters and NULL out_stream. */
static void test_stream_builder_edges(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_error_t *err = NULL;

    CHECK_EQ_INT(zerobus_stream_builder_set_table(NULL, sv("c.s.t"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(
        zerobus_stream_builder_set_oauth(NULL, sv("id"), sv("secret"), &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_builder_new(sdk, &stb, NULL);

    /* NULL out_stream. */
    zerobus_stream_builder_set_table(stb, sv("c.s.t"), NULL);
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);

    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
}

/* set_oauth is transactional: a second valid call replaces (and frees) the
 * previous credentials, and a rejected call leaves them in place. */
static void test_stream_oauth_transactional(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_builder_new(sdk, &stb, NULL);
    zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL);

    /* A second valid set_oauth replaces (and frees) the first. */
    CHECK_OK(
        zerobus_stream_builder_set_oauth(stb, sv("id1"), sv("sec1"), NULL));
    CHECK_OK(
        zerobus_stream_builder_set_oauth(stb, sv("id2"), sv("sec2"), NULL));

    /* A rejected set_oauth must not clobber the credentials already set. */
    CHECK_EQ_INT(
        zerobus_stream_builder_set_oauth(stb, sv(""), sv("sec3"), NULL),
        ZEROBUS_STATUS_INVALID_ARGUMENT);

    /* Build still succeeds using the credentials set before the failed call. */
    zerobus_stream_t *stream = NULL;
    CHECK_OK(zerobus_stream_builder_build(stb, &stream, NULL));
    CHECK(stream != NULL);

    zerobus_stream_free(stream);
    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
}

/* Every option setter accepts a value, max in-flight requests is bounded, and
 * the size limit carries over to the built stream. */
static void test_stream_options(void)
{
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_error_t *err = NULL;

    sdk = make_sdk();
    REQUIRE_OK(zerobus_stream_builder_new(sdk, &stb, NULL));
    REQUIRE_OK(zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL));
    REQUIRE_OK(
        zerobus_stream_builder_set_oauth(stb, sv("id"), sv("secret"), NULL));

    CHECK_OK(zerobus_stream_builder_set_max_inflight_requests(
        stb, (size_t)1 << 24, NULL));
    CHECK_OK(
        zerobus_stream_builder_set_max_inflight_requests(stb, 1000000, NULL));
    CHECK_OK(zerobus_stream_builder_set_max_inflight_requests(stb, 1, NULL));
    CHECK_OK(zerobus_stream_builder_set_recovery(stb, false, NULL));
    CHECK_OK(zerobus_stream_builder_set_recovery_timeout_ms(stb, 1, NULL));
    CHECK_OK(zerobus_stream_builder_set_recovery_backoff_ms(stb, 1, NULL));
    CHECK_OK(zerobus_stream_builder_set_recovery_retries(stb, 1, NULL));
    CHECK_OK(
        zerobus_stream_builder_set_server_lack_of_ack_timeout_ms(stb, 1, NULL));
    CHECK_OK(zerobus_stream_builder_set_flush_timeout_ms(stb, 1, NULL));
    CHECK_OK(zerobus_stream_builder_set_stream_paused_max_wait_time_ms(stb, 0,
                                                                       NULL));
    CHECK_OK(zerobus_stream_builder_set_max_ingest_payload_bytes(stb, 4, NULL));
    /* Room for the build to connect and the test to ingest. */
    CHECK_OK(zerobus_stream_builder_set_recovery_timeout_ms(stb, 10000, NULL));
    CHECK_OK(zerobus_stream_builder_set_flush_timeout_ms(stb, 10000, NULL));
    CHECK_OK(zerobus_stream_builder_set_server_lack_of_ack_timeout_ms(
        stb, 10000, NULL));

    /* Outside 1 to 2^24. */
    static const size_t bad_inflight[] = {0, ((size_t)1 << 24) + 1};
    for (size_t i = 0; i < sizeof(bad_inflight) / sizeof(bad_inflight[0]);
         ++i) {
        CHECK_EQ_INT(zerobus_stream_builder_set_max_inflight_requests(
                         stb, bad_inflight[i], &err),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
        CHECK(err != NULL);
        zerobus_error_free(err);
        err = NULL;
    }

    CHECK_EQ_INT(
        zerobus_stream_builder_set_max_ingest_payload_bytes(stb, 0, NULL),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_stream_builder_set_max_ingest_payload_bytes(
                     stb, SIZE_MAX, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zerobus_stream_builder_set_server_lack_of_ack_timeout_ms(stb, 0, NULL),
        ZEROBUS_STATUS_INVALID_ARGUMENT);

    REQUIRE_OK(zerobus_stream_builder_build(stb, &stream, NULL));
    CHECK_OK(zerobus_stream_ingest_json_record(stream, sv("abcd"), NULL, &err));
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_record(stream, sv("abcde"), NULL, &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* The argument checks every option setter shares. */
    CHECK_EQ_INT(
        zerobus_stream_builder_set_max_inflight_requests(NULL, 4, &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    REQUIRE(err != NULL);
    const zerobus_error_t *seeded = err;
    CHECK_EQ_INT(
        zerobus_stream_builder_set_max_ingest_payload_bytes(stb, 1, &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zerobus_stream_builder_set_server_lack_of_ack_timeout_ms(stb, 1, &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(err == seeded);

zb_cleanup:
    zerobus_error_free(err);
    zerobus_stream_free(stream);
    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
}
/* A headers provider replaces OAuth credentials, and set_oauth replaces it. */
static void test_stream_headers_provider(void)
{
    int destroys = 0;
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_headers_provider_t *provider = make_provider(&destroys);
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_error_t *err = NULL;
    REQUIRE(sdk != NULL && provider != NULL);
    REQUIRE_OK(zerobus_stream_builder_new(sdk, &stb, NULL));
    REQUIRE_OK(zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL));

    CHECK_EQ_INT(
        zerobus_stream_builder_set_headers_provider(NULL, provider, &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(err != NULL);
    CHECK_EQ_INT(zerobus_stream_builder_set_headers_provider(stb, NULL, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);

    CHECK_OK(
        zerobus_stream_builder_set_oauth(stb, sv("id"), sv("secret"), NULL));
    CHECK_OK(zerobus_stream_builder_set_headers_provider(stb, provider, NULL));
    CHECK_OK(zerobus_stream_builder_build(stb, &stream, NULL));
    zerobus_stream_free(stream);
    stream = NULL;
    CHECK_OK(
        zerobus_stream_builder_set_oauth(stb, sv("id"), sv("secret"), NULL));
    CHECK_OK(zerobus_stream_builder_build(stb, &stream, NULL));

    /* set_oauth released the builder's reference. */
    zerobus_headers_provider_free(provider);
    provider = NULL;
    CHECK_EQ_INT(destroys, 1);

zb_cleanup:
    zerobus_error_free(err);
    zerobus_stream_free(stream);
    zerobus_stream_builder_free(stb);
    zerobus_headers_provider_free(provider);
    zerobus_sdk_free(sdk);
}

/* The builder and each stream keep the provider alive until they are freed. */
static void test_stream_retains_headers_provider(void)
{
    int destroys = 0;
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_headers_provider_t *provider = make_provider(&destroys);
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_t *first = NULL;
    zerobus_stream_t *second = NULL;
    REQUIRE(sdk != NULL && provider != NULL);
    REQUIRE_OK(zerobus_stream_builder_new(sdk, &stb, NULL));
    REQUIRE_OK(zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL));
    /* Setting the same provider again keeps one builder reference. */
    REQUIRE_OK(
        zerobus_stream_builder_set_headers_provider(stb, provider, NULL));
    REQUIRE_OK(
        zerobus_stream_builder_set_headers_provider(stb, provider, NULL));
    zerobus_headers_provider_free(provider);
    provider = NULL;

    REQUIRE_OK(zerobus_stream_builder_build(stb, &first, NULL));
    REQUIRE_OK(zerobus_stream_builder_build(stb, &second, NULL));
    zerobus_stream_builder_free(stb);
    stb = NULL;
    zerobus_stream_free(first);
    first = NULL;
    CHECK_EQ_INT(destroys, 0);
    zerobus_stream_free(second);
    second = NULL;
    CHECK_EQ_INT(destroys, 1);

zb_cleanup:
    zerobus_stream_free(second);
    zerobus_stream_free(first);
    zerobus_stream_builder_free(stb);
    zerobus_headers_provider_free(provider);
    zerobus_sdk_free(sdk);
}

static void test_ingest_validation(void)
{
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_error_t *err = NULL;
    char *buf = NULL;

    sdk = make_sdk();
    stream = make_stream(sdk);
    REQUIRE(stream != NULL);

    /* NULL stream. */
    CHECK_EQ_INT(zerobus_stream_ingest_json_record(NULL, sv("{}"), NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Empty record. */
    CHECK_EQ_INT(zerobus_stream_ingest_json_record(
                     stream, (zerobus_string_view_t){NULL, 0}, NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    CHECK_OK(
        zerobus_stream_ingest_json_record(stream, sv("record"), NULL, &err));
    CHECK(err == NULL);
    CHECK_OK(zerobus_stream_ingest_json_record(
        stream, sv("{\"id\":1,\"m\":\"hi\"}"), NULL, &err));
    CHECK(err == NULL);

    /* Valid multibyte UTF-8 and an escaped NUL pass text validation. */
    CHECK_OK(zerobus_stream_ingest_json_record(
        stream, sv("{\"k\":\"\xe2\x82\xac\"}"), NULL, &err));
    CHECK(err == NULL);
    CHECK_OK(zerobus_stream_ingest_json_record(
        stream, sv("{\"k\":\"\\u0000\"}"), NULL, &err));
    CHECK(err == NULL);

    static const char raw_nul[] = {'{', '\0', '}'};
    static const zerobus_string_view_t invalid[] = {
        {"{\"k\":\"\xff\"}", 9},
        {"\xe2\x82", 2},
        {raw_nul, sizeof(raw_nul)},
    };
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
        CHECK_EQ_INT(
            zerobus_stream_ingest_json_record(stream, invalid[i], NULL, &err),
            ZEROBUS_STATUS_INVALID_ARGUMENT);
        zerobus_error_free(err);
        err = NULL;
    }

    /* A record past the size ceiling is refused by the size gate. */
    size_t big = 11u * 1024u * 1024u;
    buf = (char *)malloc(big);
    REQUIRE(buf != NULL);
    memset(buf, 'a', big);
    CHECK_EQ_INT(zerobus_stream_ingest_json_record(
                     stream, (zerobus_string_view_t){buf, big}, NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);

zb_cleanup:
    zerobus_error_free(err);
    free(buf);
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
}

static void test_flush_close_idempotent(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_stream_t *stream = make_stream(sdk);
    CHECK(stream != NULL);

    zerobus_error_t *err = NULL;

    /* NULL stream. */
    CHECK_EQ_INT(zerobus_stream_flush(NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(zerobus_stream_close(NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    CHECK_OK(zerobus_stream_flush(stream, &err));
    CHECK_OK(zerobus_stream_close(stream, &err));
    /* Idempotent: a second close still succeeds. */
    CHECK_OK(zerobus_stream_close(stream, &err));
    CHECK(err == NULL);

    /* After close, ingest is rejected with FAILED_PRECONDITION. */
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_record(stream, sv("{\"id\":2}"), NULL, &err),
        ZEROBUS_STATUS_FAILED_PRECONDITION);
    zerobus_error_free(err);
    err = NULL;

    /* Even a malformed record: the closed check precedes record validation. */
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_record(stream, sv("{not json"), NULL, &err),
        ZEROBUS_STATUS_FAILED_PRECONDITION);
    zerobus_error_free(err);

    /* Flush after close is rejected, like ingest. */
    CHECK_EQ_INT(zerobus_stream_flush(stream, NULL),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);

    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
}

/* Every stream-side *_free accepts NULL. */
static void test_stream_free_null_safe(void)
{
    zerobus_stream_builder_free(NULL);
    zerobus_stream_free(NULL);
    CHECK(1);
}

static void test_stream_builder_retains_sdk(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_stream_builder_t *builder = NULL;
    zerobus_stream_t *first = NULL;
    zerobus_stream_t *second = NULL;

    REQUIRE_OK(zerobus_stream_builder_new(sdk, &builder, NULL));
    zerobus_sdk_free(sdk);
    sdk = NULL;
    CHECK_EQ_INT(zerobus_stream_builder_build(builder, &first, NULL),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK(first == NULL);
    REQUIRE_OK(zerobus_stream_builder_set_table(builder, sv("c.s.t"), NULL));
    REQUIRE_OK(zerobus_stream_builder_set_oauth(builder, sv("id"), sv("secret"),
                                                NULL));
    REQUIRE_OK(zerobus_stream_builder_build(builder, &first, NULL));
    REQUIRE_OK(zerobus_stream_builder_build(builder, &second, NULL));

    zerobus_stream_free(first);
    first = NULL;
    zerobus_stream_builder_free(builder);
    builder = NULL;
    CHECK_OK(zerobus_stream_flush(second, NULL));
    CHECK_OK(zerobus_stream_close(second, NULL));

zb_cleanup:
    zerobus_stream_free(second);
    zerobus_stream_free(first);
    zerobus_stream_builder_free(builder);
    zerobus_sdk_free(sdk);
}

static void test_stream_retains_sdk(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_stream_t *stream = make_stream(sdk);
    CHECK(stream != NULL);

    zerobus_sdk_free(sdk);
    CHECK_OK(zerobus_stream_ingest_json_record(stream, sv("{}"), NULL, NULL));
    CHECK_OK(zerobus_stream_close(stream, NULL));
    zerobus_stream_free(stream);
}

static void test_concurrent_sdk_child_ownership(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    struct start_gate gate = {0};
    struct ownership_context contexts[WORKERS] = {0};
    zb_thread_t threads[WORKERS];
    unsigned int created = 0;

    REQUIRE_OK(zb_mutex_init(&gate.mutex));
    gate.mutex_initialized = true;
    REQUIRE_OK(zb_cond_init(&gate.changed));
    gate.changed_initialized = true;
    for (unsigned int i = 0; i < WORKERS; i++) {
        contexts[i].gate = &gate;
        REQUIRE_OK(zerobus_stream_builder_new(sdk, &contexts[i].builder, NULL));
        REQUIRE_OK(zerobus_stream_builder_set_table(contexts[i].builder,
                                                    sv("c.s.t"), NULL));
        REQUIRE_OK(zerobus_stream_builder_set_oauth(
            contexts[i].builder, sv("id"), sv("secret"), NULL));
        /* Each of the many builds allocates a landing zone: keep it small. */
        REQUIRE_OK(zerobus_stream_builder_set_max_inflight_requests(
            contexts[i].builder, 4, NULL));
        REQUIRE_OK(zb_thread_create(&threads[i], build_and_free_streams,
                                    &contexts[i]));
        created++;
    }
    zerobus_sdk_free(sdk);
    sdk = NULL;

zb_cleanup:
    finish_workers(&gate, threads, created);
    for (unsigned int i = 0; i < created; i++) {
        CHECK_OK(contexts[i].status);
        CHECK_EQ_INT(contexts[i].completed, BUILDS);
    }
    for (unsigned int i = 0; i < WORKERS; i++) {
        zerobus_stream_builder_free(contexts[i].builder);
    }
    zerobus_sdk_free(sdk);
}

static void test_concurrent_ingest_flush_close(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_stream_t *stream = make_stream(sdk);
    struct start_gate gate = {0};
    struct stream_context contexts[WORKERS] = {0};
    zb_thread_t threads[WORKERS];
    unsigned int created = 0;

    REQUIRE(stream != NULL);
    REQUIRE_OK(zb_mutex_init(&gate.mutex));
    gate.mutex_initialized = true;
    REQUIRE_OK(zb_cond_init(&gate.changed));
    gate.changed_initialized = true;
    for (unsigned int i = 0; i < WORKERS; i++) {
        contexts[i].gate = &gate;
        contexts[i].stream = stream;
        contexts[i].operation = i == 0   ? CALL_FLUSH
                                : i == 1 ? CALL_CLOSE
                                         : CALL_INGEST;
        REQUIRE_OK(zb_thread_create(&threads[i], call_stream, &contexts[i]));
        created++;
    }

zb_cleanup:
    finish_workers(&gate, threads, created);
    for (unsigned int i = 0; i < created; i++) {
        CHECK_OK(contexts[i].unexpected_status);
        CHECK_EQ_INT(contexts[i].completed, ITERATIONS);
    }
    if (created == WORKERS) {
        CHECK_EQ_INT(
            zerobus_stream_ingest_json_record(stream, sv("{}"), NULL, NULL),
            ZEROBUS_STATUS_FAILED_PRECONDITION);
        CHECK_EQ_INT(zerobus_stream_flush(stream, NULL),
                     ZEROBUS_STATUS_FAILED_PRECONDITION);
        CHECK_OK(zerobus_stream_close(stream, NULL));
    }
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
}

/* A non-NULL *out_error on entry is refused at every entry point, and the
 * existing error is left untouched (not overwritten, freed, or replaced). */
static void test_stream_out_error_must_be_null(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_stream_t *stream = make_stream(sdk);

    zerobus_error_t *err = NULL;
    /* Seed a live error through a genuine failure. */
    zerobus_stream_builder_t *stb = NULL;
    CHECK_EQ_INT(zerobus_stream_builder_new(NULL, &stb, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(err != NULL);
    const zerobus_error_t *seeded = err;

    /* The guard runs first, so the other arguments do not matter. */
    zerobus_stream_t *st = NULL;
    CHECK_EQ_INT(zerobus_stream_builder_new(sdk, &stb, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_stream_builder_set_table(stb, sv("c.s.t"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zerobus_stream_builder_set_oauth(stb, sv("id"), sv("secret"), &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_stream_builder_set_headers_provider(stb, NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &st, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_record(stream, sv("{\"id\":1}"), NULL, &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_records(stream, NULL, 0, NULL, &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_stream_wait_for_offset(stream, 0, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_stream_flush(stream, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_stream_close(stream, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);

    CHECK(err == seeded); /* same object: untouched */
    CHECK(stb == NULL);
    CHECK(st == NULL);
    zerobus_error_free(err);

    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
}

/* The realloc-style out-handle contract: a failed call leaves the caller's
 * out-handle untouched — the handle is written only on success. */
static void test_stream_out_handle_untouched_on_failure(void)
{
    zerobus_sdk_t *sdk = make_sdk();

    /* A sentinel we never dereference: any non-NULL value distinct from a real
     * handle works, so point it at a local object. */
    int marker;
    zerobus_stream_builder_t *const builder_sentinel =
        (zerobus_stream_builder_t *)&marker;
    zerobus_stream_t *const stream_sentinel = (zerobus_stream_t *)&marker;

    /* builder_new rejected for a NULL sdk: out_builder is not touched. */
    zerobus_error_t *err = NULL;
    zerobus_stream_builder_t *stb = builder_sentinel;
    CHECK_EQ_INT(zerobus_stream_builder_new(NULL, &stb, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(stb == builder_sentinel);
    zerobus_error_free(err);
    err = NULL;

    /* A real builder with nothing set: build fails the precondition and must
     * not touch out_stream. */
    stb = NULL;
    REQUIRE_OK(zerobus_stream_builder_new(sdk, &stb, NULL));
    zerobus_stream_t *stream = stream_sentinel;
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &stream, &err),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK(stream == stream_sentinel);
    zerobus_error_free(err);
    err = NULL;

    /* build rejected for a NULL builder: out_stream still untouched. */
    stream = stream_sentinel;
    CHECK_EQ_INT(zerobus_stream_builder_build(NULL, &stream, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(stream == stream_sentinel);

zb_cleanup:
    zerobus_error_free(err);
    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
}

/* ---- delivery ---------------------------------------------------------- */

/* Authenticates stb with a provider that sends no headers, which the fake
 * service accepts. */
static zerobus_status_t set_empty_provider(zerobus_stream_builder_t *stb)
{
    zerobus_headers_provider_t *provider = NULL;
    zerobus_status_t status =
        zerobus_static_headers_provider_new(NULL, NULL, 0, &provider, NULL);
    if (status == ZEROBUS_STATUS_OK) {
        status =
            zerobus_stream_builder_set_headers_provider(stb, provider, NULL);
    }
    zerobus_headers_provider_free(provider);
    return status;
}

/* Builds a stream to cat.sch.tbl that retries quickly. With oauth it
 * authenticates with client credentials, otherwise with set_empty_provider. */
static zerobus_stream_t *make_fast_stream(zerobus_sdk_t *sdk, bool recovery,
                                          bool oauth,
                                          zerobus_status_t *out_status)
{
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_stream_builder_new(sdk, &stb, NULL);
    zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL);
    zerobus_stream_builder_set_recovery(stb, recovery, NULL);
    zerobus_stream_builder_set_recovery_backoff_ms(stb, 1, NULL);
    zerobus_stream_builder_set_recovery_retries(stb, 2, NULL);
    if (oauth) {
        zerobus_stream_builder_set_oauth(stb, sv("id"), sv("secret"), NULL);
    } else {
        set_empty_provider(stb);
    }
    zerobus_error_t *err = NULL;
    zerobus_status_t status = zerobus_stream_builder_build(stb, &stream, &err);
    /* A failed open says why. */
    CHECK(status == ZEROBUS_STATUS_OK ? err == NULL : err != NULL);
    zerobus_error_free(err);
    zerobus_stream_builder_free(stb);
    if (out_status != NULL) {
        *out_status = status;
    }
    return stream;
}

/* Ingests count records {"id":<first + i>}, checking each offset, and adds
 * them to *digest. Stops at the first failure and returns it. */
static zerobus_status_t ingest_ids(zerobus_stream_t *stream, unsigned first,
                                   unsigned count, uint64_t *digest)
{
    for (unsigned i = first; i < first + count; ++i) {
        char record[32];
        int len = snprintf(record, sizeof(record), "{\"id\":%u}", i);
        zerobus_offset_t offset = -1;
        zerobus_status_t status = zerobus_stream_ingest_json_record(
            stream, (zerobus_string_view_t){record, (size_t)len}, &offset,
            NULL);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        CHECK_EQ_INT(offset, i);
        *digest = fake_zerobus_digest_add(*digest, record, (size_t)len);
    }
    return ZEROBUS_STATUS_OK;
}

/* Every record arrives once and in order, offsets count from 0, and flush
 * waits for cumulative acknowledgments that each cover several records. */
static void test_delivery(void)
{
    const fake_zerobus_options_t options = {.ack_every = 7};
    fake_zerobus_t *service = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;
    uint64_t digest = FAKE_ZEROBUS_DIGEST_SEED;
    REQUIRE_OK(fake_zerobus_start(&options, &service));
    sdk = make_sdk_for(service);
    stream = make_fast_stream(sdk, true, false, NULL);
    REQUIRE(stream != NULL);

    CHECK_OK(zerobus_stream_flush(stream, NULL)); /* Nothing to wait for. */
    REQUIRE_OK(ingest_ids(stream, 0, 7 * 140, &digest));
    CHECK_OK(zerobus_stream_flush(stream, NULL));
    CHECK_EQ_INT(fake_zerobus_records(service), 7 * 140);
    CHECK(fake_zerobus_digest(service) == digest);
    REQUIRE_OK(ingest_ids(stream, 7 * 140, 7, &digest));
    CHECK_OK(zerobus_stream_close(stream, NULL));
    CHECK_EQ_INT(fake_zerobus_records(service), 7 * 141);
    CHECK(fake_zerobus_digest(service) == digest);
    CHECK_EQ_INT(fake_zerobus_calls(service), 1);

zb_cleanup:
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(service);
}

/* A call that fails is replaced, and the records it left unacknowledged are
 * sent again on the new call. */
static void test_recovery(void)
{
    const fake_zerobus_options_t options = {.fail_after = 100, .fail_calls = 1};
    fake_zerobus_t *service = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;
    uint64_t digest = FAKE_ZEROBUS_DIGEST_SEED;
    REQUIRE_OK(fake_zerobus_start(&options, &service));
    sdk = make_sdk_for(service);
    stream = make_fast_stream(sdk, true, false, NULL);
    REQUIRE(stream != NULL);

    REQUIRE_OK(ingest_ids(stream, 0, 1000, &digest));
    CHECK_OK(zerobus_stream_flush(stream, NULL));
    CHECK_OK(zerobus_stream_close(stream, NULL));
    CHECK_EQ_INT(fake_zerobus_calls(service), 2);
    /* At least once: the unacknowledged ones arrived twice. */
    CHECK(fake_zerobus_records(service) > 1000);

zb_cleanup:
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(service);
}

/* Ingests count records {"id":<first + i>} as one batch, which must get
 * offset expected, and adds them to *digest. */
static zerobus_status_t ingest_batch(zerobus_stream_t *stream, unsigned first,
                                     unsigned count, zerobus_offset_t expected,
                                     uint64_t *digest)
{
    char (*text)[32] = calloc(count ? count : 1, sizeof(*text));
    zerobus_string_view_t *records =
        calloc(count ? count : 1, sizeof(*records));
    zerobus_status_t status = ZEROBUS_STATUS_OUT_OF_MEMORY;
    if (text != NULL && records != NULL) {
        for (unsigned i = 0; i < count; ++i) {
            int len =
                snprintf(text[i], sizeof(text[i]), "{\"id\":%u}", first + i);
            records[i] = (zerobus_string_view_t){text[i], (size_t)len};
        }
        zerobus_offset_t offset = -2;
        status = zerobus_stream_ingest_json_records(stream, records, count,
                                                    &offset, NULL);
        if (status == ZEROBUS_STATUS_OK) {
            CHECK_EQ_INT(offset, expected);
            for (unsigned i = 0; i < count; ++i) {
                *digest = fake_zerobus_digest_add(*digest, records[i].data,
                                                  records[i].len);
            }
        }
    }
    free(records);
    free(text);
    return status;
}

/* A batch takes one offset, its records arrive in order among single ones,
 * an empty batch takes none, and a batch of one is sent as a single record. */
static void test_batch_delivery(void)
{
    fake_zerobus_t *service = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;
    uint64_t digest = FAKE_ZEROBUS_DIGEST_SEED;
    REQUIRE_OK(fake_zerobus_start(NULL, &service));
    sdk = make_sdk_for(service);
    stream = make_fast_stream(sdk, true, false, NULL);
    REQUIRE(stream != NULL);

    REQUIRE_OK(ingest_ids(stream, 0, 1, &digest));
    REQUIRE_OK(ingest_batch(stream, 1, 3, 1, &digest));
    REQUIRE_OK(ingest_batch(stream, 4, 0, -1, &digest));
    REQUIRE_OK(ingest_batch(stream, 4, 1, 2, &digest));
    REQUIRE_OK(ingest_batch(stream, 5, 1000, 3, &digest));
    CHECK_OK(zerobus_stream_flush(stream, NULL));
    CHECK_EQ_INT(fake_zerobus_records(service), 1005);
    CHECK(fake_zerobus_digest(service) == digest);
    CHECK_OK(zerobus_stream_ingest_json_records(stream, NULL, 0, NULL, NULL));
    CHECK_OK(zerobus_stream_close(stream, NULL));

zb_cleanup:
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(service);
}

/* A batch with an invalid record, or over the payload limit, admits
 * nothing. */
/* wait_for_offset returns once the server acknowledges the offset an ingest
 * returned, and refuses an offset no ingest returned and a closed stream. */
static void test_wait_for_offset(void)
{
    const fake_zerobus_options_t options = {.ack_every = 4};
    fake_zerobus_t *service = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_offset_t offset = -1;
    REQUIRE_OK(fake_zerobus_start(&options, &service));
    sdk = make_sdk_for(service);
    stream = make_fast_stream(sdk, true, false, NULL);
    REQUIRE(stream != NULL);

    CHECK_EQ_INT(zerobus_stream_wait_for_offset(stream, 0, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    for (int i = 0; i < 8; ++i) {
        CHECK_OK(zerobus_stream_ingest_json_record(stream, sv("{\"id\":1}"),
                                                   &offset, NULL));
    }
    CHECK_EQ_INT(offset, 7);
    /* The server acknowledges every fourth record. */
    CHECK_OK(zerobus_stream_wait_for_offset(stream, 3, NULL));
    CHECK_OK(zerobus_stream_wait_for_offset(stream, offset, NULL));
    CHECK_EQ_INT(fake_zerobus_records(service), 8);
    CHECK_EQ_INT(zerobus_stream_wait_for_offset(stream, offset + 1, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zerobus_stream_wait_for_offset(stream, -1, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_OK(zerobus_stream_close(stream, NULL));
    CHECK_EQ_INT(zerobus_stream_wait_for_offset(stream, 0, NULL),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK_EQ_INT(zerobus_stream_wait_for_offset(NULL, 0, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);

zb_cleanup:
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(service);
}

static void test_batch_validation(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_error_t *err = NULL;
    zerobus_offset_t offset = -2;
    REQUIRE(sdk != NULL);
    REQUIRE_OK(zerobus_stream_builder_new(sdk, &stb, NULL));
    REQUIRE_OK(zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL));
    REQUIRE_OK(set_empty_provider(stb));
    REQUIRE_OK(
        zerobus_stream_builder_set_max_ingest_payload_bytes(stb, 10, NULL));
    REQUIRE_OK(zerobus_stream_builder_build(stb, &stream, NULL));

    const zerobus_string_view_t fits[] = {sv("{\"a\":1}"), sv("{}")};
    const zerobus_string_view_t too_big[] = {sv("{\"a\":1}"), sv("{\"b\":2}")};
    const zerobus_string_view_t invalid[] = {sv("{}"), {"\xff", 1}};
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_records(NULL, fits, 2, &offset, &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_records(stream, NULL, 2, &offset, NULL),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_records(stream, too_big, 2, &offset, &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(err != NULL);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_records(stream, invalid, 2, &offset, NULL),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(offset, -2);
    /* Nothing was admitted, so the first good batch takes offset 0. */
    CHECK_OK(
        zerobus_stream_ingest_json_records(stream, fits, 2, &offset, NULL));
    CHECK_EQ_INT(offset, 0);
    CHECK_OK(zerobus_stream_close(stream, NULL));
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_records(stream, fits, 0, &offset, NULL),
        ZEROBUS_STATUS_FAILED_PRECONDITION);

zb_cleanup:
    zerobus_error_free(err);
    zerobus_stream_free(stream);
    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
}

/* Recovery resends unacknowledged batches whole. */
static void test_batch_recovery(void)
{
    const fake_zerobus_options_t options = {.fail_after = 55, .fail_calls = 1};
    fake_zerobus_t *service = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;
    uint64_t digest = FAKE_ZEROBUS_DIGEST_SEED;
    REQUIRE_OK(fake_zerobus_start(&options, &service));
    sdk = make_sdk_for(service);
    stream = make_fast_stream(sdk, true, false, NULL);
    REQUIRE(stream != NULL);

    for (unsigned i = 0; i < 20; ++i) {
        REQUIRE_OK(ingest_batch(stream, i * 10, 10, i, &digest));
    }
    CHECK_OK(zerobus_stream_flush(stream, NULL));
    CHECK_OK(zerobus_stream_close(stream, NULL));
    CHECK_EQ_INT(fake_zerobus_calls(service), 2);
    CHECK(fake_zerobus_records(service) > 200);

zb_cleanup:
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(service);
}

/* Without recovery the first failed call fails the stream for good. */
static void test_failure_without_recovery(void)
{
    const fake_zerobus_options_t options = {.fail_after = 10, .fail_calls = 1};
    fake_zerobus_t *service = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_error_t *err = NULL;
    uint64_t digest = FAKE_ZEROBUS_DIGEST_SEED;
    REQUIRE_OK(fake_zerobus_start(&options, &service));
    sdk = make_sdk_for(service);
    stream = make_fast_stream(sdk, false, false, NULL);
    REQUIRE(stream != NULL);

    zerobus_status_t status = ingest_ids(stream, 0, 100, &digest);
    CHECK(status == ZEROBUS_STATUS_OK || status == ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zerobus_stream_flush(stream, &err),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(err != NULL);
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_record(stream, sv("{}"), NULL, NULL),
        ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zerobus_stream_close(stream, NULL),
                 ZEROBUS_STATUS_UNAVAILABLE);
    /* A later close reports the first one's failure. */
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(zerobus_stream_close(stream, &err),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(err != NULL && zerobus_error_message(err).len != 0);
    CHECK_EQ_INT(fake_zerobus_calls(service), 1);

zb_cleanup:
    zerobus_error_free(err);
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(service);
}

/* A refused open fails the build after the retries recovery allows, and a
 * refused credential is retried once. */
static void test_open_refused(void)
{
    static const struct {
        uint32_t grpc_status;
        zerobus_status_t status;
        unsigned calls;
    } cases[] = {
        {14, ZEROBUS_STATUS_UNAVAILABLE, 3},
        {7, ZEROBUS_STATUS_PERMISSION_DENIED, 2},
        {5, ZEROBUS_STATUS_NOT_FOUND, 1},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        const fake_zerobus_options_t options = {.refuse_status =
                                                    cases[i].grpc_status};
        fake_zerobus_t *service = NULL;
        zerobus_sdk_t *sdk = NULL;
        zerobus_status_t status = ZEROBUS_STATUS_OK;
        REQUIRE_OK(fake_zerobus_start(&options, &service));
        sdk = make_sdk_for(service);
        CHECK(make_fast_stream(sdk, true, false, &status) == NULL);
        CHECK_EQ_INT(status, cases[i].status);
        CHECK_EQ_INT(fake_zerobus_calls(service), cases[i].calls);
        zerobus_sdk_free(sdk);
        fake_zerobus_stop(service);
    }
zb_cleanup:
    return;
}

/* OAuth credentials get a token from Unity Catalog, which every call sends,
 * and a stream without them is refused. */
static void test_oauth(void)
{
    const fake_zerobus_options_t options = {.authorization =
                                                "Bearer fake-token"};
    fake_zerobus_t *service = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    uint64_t digest = FAKE_ZEROBUS_DIGEST_SEED;
    REQUIRE_OK(fake_zerobus_start(&options, &service));
    sdk = make_sdk_for(service);

    CHECK(make_fast_stream(sdk, true, false, &status) == NULL);
    CHECK_EQ_INT(status, ZEROBUS_STATUS_UNAUTHENTICATED);
    stream = make_fast_stream(sdk, true, true, NULL);
    REQUIRE(stream != NULL);
    REQUIRE_OK(ingest_ids(stream, 0, 10, &digest));
    CHECK_OK(zerobus_stream_close(stream, NULL));
    CHECK(fake_zerobus_digest(service) == digest);
    CHECK_EQ_INT(fake_zerobus_tokens(service), 1);

zb_cleanup:
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(service);
}

/* Records the server leaves unacknowledged for too long fail the stream. */
/* Without acknowledgments, flush, wait_for_offset and close stop at the flush
 * timeout. */
static void test_flush_timeout(void)
{
    const fake_zerobus_options_t options = {.ack_every = 1000};
    fake_zerobus_t *service = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_error_t *err = NULL;
    REQUIRE_OK(fake_zerobus_start(&options, &service));
    sdk = make_sdk_for(service);
    REQUIRE_OK(zerobus_stream_builder_new(sdk, &stb, NULL));
    REQUIRE_OK(zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL));
    REQUIRE_OK(set_empty_provider(stb));
    REQUIRE_OK(zerobus_stream_builder_set_flush_timeout_ms(stb, 50, NULL));
    REQUIRE_OK(zerobus_stream_builder_build(stb, &stream, NULL));
    REQUIRE_OK(zerobus_stream_ingest_json_record(stream, sv("{}"), NULL, NULL));

    CHECK_EQ_INT(zerobus_stream_flush(stream, &err),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK(err != NULL);
    CHECK_EQ_INT(zerobus_stream_wait_for_offset(stream, 0, NULL),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    /* A close whose flush times out aborts the stream. */
    CHECK_EQ_INT(zerobus_stream_close(stream, NULL),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);

zb_cleanup:
    zerobus_error_free(err);
    zerobus_stream_free(stream);
    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(service);
}

static void test_lack_of_ack(void)
{
    const fake_zerobus_options_t options = {.ack_every = 1000};
    fake_zerobus_t *service = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_t *stream = NULL;
    REQUIRE_OK(fake_zerobus_start(&options, &service));
    sdk = make_sdk_for(service);
    REQUIRE_OK(zerobus_stream_builder_new(sdk, &stb, NULL));
    REQUIRE_OK(zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL));
    REQUIRE_OK(set_empty_provider(stb));
    REQUIRE_OK(zerobus_stream_builder_set_recovery(stb, false, NULL));
    REQUIRE_OK(zerobus_stream_builder_set_server_lack_of_ack_timeout_ms(stb, 50,
                                                                        NULL));
    REQUIRE_OK(zerobus_stream_builder_build(stb, &stream, NULL));
    REQUIRE_OK(zerobus_stream_ingest_json_record(stream, sv("{}"), NULL, NULL));
    CHECK_EQ_INT(zerobus_stream_flush(stream, NULL),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);

zb_cleanup:
    zerobus_stream_free(stream);
    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(service);
}

/* Streams that would share one connection are refused for now. */
static void test_shared_connection_unimplemented(void)
{
    zerobus_sdk_builder_t *b = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_t *stream = NULL;
    REQUIRE_OK(zerobus_sdk_builder_new(&b, NULL));
    REQUIRE_OK(
        zerobus_sdk_builder_set_endpoint(b, sv("http://127.0.0.1:1"), NULL));
    REQUIRE_OK(zerobus_sdk_builder_set_unity_catalog_endpoint(
        b, sv("http://127.0.0.1:1"), NULL));
    REQUIRE_OK(zerobus_sdk_builder_set_connection_per_stream(b, false, NULL));
    REQUIRE_OK(zerobus_sdk_builder_build(b, &sdk, NULL));
    REQUIRE_OK(zerobus_stream_builder_new(sdk, &stb, NULL));
    REQUIRE_OK(zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL));
    REQUIRE_OK(set_empty_provider(stb));
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &stream, NULL),
                 ZEROBUS_STATUS_UNIMPLEMENTED);

zb_cleanup:
    zerobus_stream_free(stream);
    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
    zerobus_sdk_builder_free(b);
}

int main(void)
{
    if (fake_zerobus_start(NULL, &server) != ZEROBUS_STATUS_OK) {
        fprintf(stderr, "starting the fake Zerobus service failed\n");
        return 1;
    }
    test_stream_builder_validation();
    test_stream_builder_edges();
    test_stream_oauth_transactional();
    test_stream_options();
    test_stream_headers_provider();
    test_stream_retains_headers_provider();
    test_ingest_validation();
    test_flush_close_idempotent();
    test_stream_free_null_safe();
    test_stream_builder_retains_sdk();
    test_stream_retains_sdk();
    test_concurrent_sdk_child_ownership();
    test_concurrent_ingest_flush_close();
    test_stream_out_error_must_be_null();
    test_stream_out_handle_untouched_on_failure();
    test_delivery();
    test_recovery();
    test_batch_delivery();
    test_wait_for_offset();
    test_batch_validation();
    test_batch_recovery();
    test_failure_without_recovery();
    test_open_refused();
    test_oauth();
    test_flush_timeout();
    test_lack_of_ack();
    test_shared_connection_unimplemented();
    fake_zerobus_stop(server);
    TEST_MAIN_RETURN();
}
