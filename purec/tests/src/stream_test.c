/* Unit tests for the stream builder and stream handle (stream.c). */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fake_zerobus.h"
#include "internal/platform/concurrency.h"
#include "test_common.h"

/* ---- helpers ----------------------------------------------------------- */

/* The service most tests use, with default behavior. */
static fake_zerobus_t *service;

/* Build an SDK whose endpoints are server's. Caller frees it. */
static zerobus_sdk_t *make_sdk_for(fake_zerobus_t *server)
{
    char endpoint[64], uc_endpoint[64];
    snprintf(endpoint, sizeof(endpoint), "http://localhost:%u",
             (unsigned)fake_zerobus_port(server));
    snprintf(uc_endpoint, sizeof(uc_endpoint), "http://localhost:%u",
             (unsigned)fake_zerobus_token_port(server));
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
    return make_sdk_for(service);
}

/* A builder for table with OAuth credentials. Caller frees it. */
static zerobus_stream_builder_t *make_builder(zerobus_sdk_t *sdk)
{
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_builder_new(sdk, &stb, NULL);
    zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL);
    zerobus_stream_builder_set_oauth(stb, sv("client-id"), sv("client-secret"),
                                     NULL);
    return stb;
}

/* Build a valid stream for the ingest/flush/close tests. Caller frees the
 * stream and the SDK passed in. */
static zerobus_stream_t *make_stream(zerobus_sdk_t *sdk)
{
    zerobus_stream_builder_t *stb = make_builder(sdk);
    zerobus_stream_t *stream = NULL;
    zerobus_stream_builder_build(stb, &stream, NULL);
    zerobus_stream_builder_free(stb);
    return stream;
}

/* Ingests count small records, stopping at the first failure. */
static zerobus_status_t ingest_records(zerobus_stream_t *stream, unsigned count)
{
    for (unsigned i = 0; i < count; ++i) {
        char record[48];
        int len = snprintf(record, sizeof(record), "{\"id\":%u}", i);
        zerobus_status_t status = zerobus_stream_ingest_json_record(
            stream, (zerobus_string_view_t){record, (size_t)len}, NULL, NULL);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
    }
    return ZEROBUS_STATUS_OK;
}

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

    /* Table set but no credentials: still fails the precondition. */
    zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL);
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &stream, &err),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
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

    zerobus_offset_t offset = -1;
    CHECK_OK(
        zerobus_stream_ingest_json_record(stream, sv("record"), &offset, &err));
    CHECK_EQ_INT(offset, 0);
    CHECK(err == NULL);
    CHECK_OK(zerobus_stream_ingest_json_record(
        stream, sv("{\"id\":1,\"m\":\"hi\"}"), &offset, &err));
    CHECK_EQ_INT(offset, 1);
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
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &st, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_record(stream, sv("{\"id\":1}"), NULL, &err),
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

/* Records admitted before flush are acknowledged, singly or in a batch with
 * one offset, and close then ends the call cleanly. */
static void test_flush_acknowledges_records(void)
{
    fake_zerobus_t *server = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;

    REQUIRE_OK(fake_zerobus_start(NULL, &server));
    sdk = make_sdk_for(server);
    stream = make_stream(sdk);
    REQUIRE(stream != NULL);
    CHECK_OK(ingest_records(stream, 1000));
    const zerobus_string_view_t batch[3] = {sv("{\"a\":1}"), sv("{\"a\":2}"),
                                            sv("{\"a\":3}")};
    zerobus_offset_t offset = -1;
    CHECK_OK(
        zerobus_stream_ingest_json_records(stream, batch, 3, &offset, NULL));
    CHECK_EQ_INT(offset, 1000);
    CHECK_OK(
        zerobus_stream_ingest_json_records(stream, batch, 0, &offset, NULL));
    CHECK_EQ_INT(offset, -1);
    CHECK_OK(zerobus_stream_flush(stream, NULL));
    CHECK_EQ_INT(fake_zerobus_records(server), 1003);
    CHECK_OK(zerobus_stream_close(stream, NULL));
    CHECK_EQ_INT(fake_zerobus_calls(server), 1);
    CHECK_EQ_INT(fake_zerobus_tokens(server), 1);

zb_cleanup:
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(server);
}

/* The acknowledged offset is -1 before any acknowledgment, and reaches an
 * ingested batch's offset without a flush. */
static void test_acked_offset_without_waiting(void)
{
    fake_zerobus_t *server = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_error_t *err = NULL;
    zerobus_offset_t acked = 0;

    REQUIRE_OK(fake_zerobus_start(NULL, &server));
    sdk = make_sdk_for(server);
    stream = make_stream(sdk);
    REQUIRE(stream != NULL);
    CHECK_EQ_INT(zerobus_stream_get_acked_offset(NULL, &acked, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(zerobus_stream_get_acked_offset(stream, NULL, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_OK(zerobus_stream_get_acked_offset(stream, &acked, NULL));
    CHECK_EQ_INT(acked, -1);

    CHECK_OK(ingest_records(stream, 100));
    const zerobus_string_view_t batch[2] = {sv("{\"a\":1}"), sv("{\"a\":2}")};
    zerobus_offset_t offset = -1;
    CHECK_OK(
        zerobus_stream_ingest_json_records(stream, batch, 2, &offset, NULL));
    CHECK_EQ_INT(offset, 100);
    zb_deadline_t deadline = zb_deadline_after_ms(10000);
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    while (status == ZEROBUS_STATUS_OK && acked < offset &&
           !zb_deadline_expired(deadline)) {
        status = zerobus_stream_get_acked_offset(stream, &acked, NULL);
    }
    CHECK_OK(status);
    CHECK_EQ_INT(acked, offset);
    CHECK_EQ_INT(fake_zerobus_records(server), 102);
    CHECK_OK(zerobus_stream_close(stream, NULL));
    /* Still readable after close. */
    CHECK_OK(zerobus_stream_get_acked_offset(stream, &acked, NULL));
    CHECK_EQ_INT(acked, offset);

zb_cleanup:
    zerobus_error_free(err);
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(server);
}

/* Records longer than a transport buffer, and records whose framing straddles
 * two buffers, arrive intact and in order. */
static void test_records_span_buffers(void)
{
    enum { COUNT = 600, BIG = 200 * 1024 };
    fake_zerobus_t *server = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;
    char *record = NULL;

    REQUIRE_OK(fake_zerobus_start(NULL, &server));
    sdk = make_sdk_for(server);
    stream = make_stream(sdk);
    REQUIRE(stream != NULL);
    record = malloc(BIG);
    REQUIRE(record != NULL);
    uint64_t digest = FAKE_ZEROBUS_DIGEST_SEED;
    for (unsigned i = 0; i < COUNT; ++i) {
        size_t len = i == 0 || i == COUNT - 1 ? BIG : 8 + (i * 997u) % 3000u;
        memcpy(record, "{\"p\":\"", 6);
        for (size_t j = 6; j < len - 2; ++j) {
            record[j] = (char)('a' + (i + j) % 26);
        }
        memcpy(record + len - 2, "\"}", 2);
        CHECK_OK(zerobus_stream_ingest_json_record(
            stream, (zerobus_string_view_t){record, len}, NULL, NULL));
        digest = fake_zerobus_digest_add(digest, record, len);
    }
    CHECK_OK(zerobus_stream_flush(stream, NULL));
    CHECK_EQ_INT(fake_zerobus_records(server), COUNT);
    CHECK(fake_zerobus_digest(server) == digest);
    CHECK_OK(zerobus_stream_close(stream, NULL));

zb_cleanup:
    free(record);
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(server);
}

/* A server refusing the stream fails build with its status, without
 * retrying a non-retryable one. (An auth rejection gets one retry on the
 * first open, to refresh a stale credential.) */
static void test_build_reports_refusal(void)
{
    fake_zerobus_t *server = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_error_t *err = NULL;

    const fake_zerobus_options_t options = {.refuse_status = 5};
    REQUIRE_OK(fake_zerobus_start(&options, &server));
    sdk = make_sdk_for(server);
    stb = make_builder(sdk);
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &stream, &err),
                 ZEROBUS_STATUS_NOT_FOUND);
    CHECK(stream == NULL);
    CHECK(err != NULL);
    CHECK_EQ_INT(fake_zerobus_calls(server), 1);

zb_cleanup:
    zerobus_error_free(err);
    zerobus_stream_free(stream);
    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(server);
}

/* A headers provider replaces OAuth, and its credentials reach the server;
 * a rejected credential fails build as UNAUTHENTICATED. */
static void test_headers_provider_authorizes(void)
{
    fake_zerobus_t *server = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_builder_t *stb = NULL;
    zerobus_headers_provider_t *provider = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_stream_t *rejected = NULL;

    const fake_zerobus_options_t options = {.authorization = "Bearer abc"};
    REQUIRE_OK(fake_zerobus_start(&options, &server));
    sdk = make_sdk_for(server);
    stb = make_builder(sdk);
    REQUIRE_OK(zerobus_stream_builder_set_recovery(stb, false, NULL));
    const zerobus_header_t good = {sv("authorization"), sv("Bearer abc")};
    REQUIRE_OK(zerobus_headers_provider_new(&good, 1, &provider, NULL));
    REQUIRE_OK(
        zerobus_stream_builder_set_headers_provider(stb, provider, NULL));
    zerobus_headers_provider_free(provider); /* The builder keeps it. */
    provider = NULL;
    CHECK_OK(zerobus_stream_builder_build(stb, &stream, NULL));
    CHECK_OK(ingest_records(stream, 10));
    CHECK_OK(zerobus_stream_flush(stream, NULL));
    CHECK_EQ_INT(fake_zerobus_tokens(server), 0);

    const zerobus_header_t bad = {sv("authorization"), sv("Bearer nope")};
    REQUIRE_OK(zerobus_headers_provider_new(&bad, 1, &provider, NULL));
    REQUIRE_OK(
        zerobus_stream_builder_set_headers_provider(stb, provider, NULL));
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &rejected, NULL),
                 ZEROBUS_STATUS_UNAUTHENTICATED);
    CHECK(rejected == NULL);

    /* Invalid header names are refused when the provider is created. */
    zerobus_headers_provider_t *invalid = NULL;
    const zerobus_header_t upper = {sv("Authorization"), sv("x")};
    CHECK_EQ_INT(zerobus_headers_provider_new(&upper, 1, &invalid, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(invalid == NULL);

zb_cleanup:
    zerobus_stream_free(rejected);
    zerobus_stream_free(stream);
    zerobus_headers_provider_free(provider);
    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(server);
}

/* After the server fails the call, recovery opens a new one and resends the
 * unacknowledged records, so flush still succeeds. */
static void test_recovery_resends_unacknowledged(void)
{
    fake_zerobus_t *server = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;

    const fake_zerobus_options_t options = {.fail_after = 500, .fail_calls = 1};
    REQUIRE_OK(fake_zerobus_start(&options, &server));
    sdk = make_sdk_for(server);
    stream = make_stream(sdk);
    REQUIRE(stream != NULL);
    CHECK_OK(ingest_records(stream, 2000));
    CHECK_OK(zerobus_stream_flush(stream, NULL));
    CHECK_EQ_INT(fake_zerobus_calls(server), 2);
    CHECK(fake_zerobus_records(server) >= 2000);
    CHECK_OK(zerobus_stream_close(stream, NULL));

zb_cleanup:
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(server);
}

/* Without recovery the failure is terminal: flush, ingest and close report
 * it. */
static void test_failure_without_recovery(void)
{
    fake_zerobus_t *server = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_error_t *err = NULL;

    const fake_zerobus_options_t options = {.fail_after = 100, .fail_calls = 1};
    REQUIRE_OK(fake_zerobus_start(&options, &server));
    sdk = make_sdk_for(server);
    stb = make_builder(sdk);
    REQUIRE_OK(zerobus_stream_builder_set_recovery(stb, false, NULL));
    REQUIRE_OK(zerobus_stream_builder_build(stb, &stream, NULL));
    zerobus_status_t status = ingest_records(stream, 1000);
    CHECK(status == ZEROBUS_STATUS_OK || status == ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zerobus_stream_flush(stream, &err),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(err != NULL);
    zerobus_error_free(err);
    err = NULL;
    zerobus_offset_t acked = -2;
    CHECK_EQ_INT(zerobus_stream_get_acked_offset(stream, &acked, &err),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(err != NULL);
    CHECK(acked >= -1 && acked < 1000);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_record(stream, sv("{}"), NULL, NULL),
        ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zerobus_stream_close(stream, NULL),
                 ZEROBUS_STATUS_UNAVAILABLE);

zb_cleanup:
    zerobus_error_free(err);
    zerobus_stream_free(stream);
    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
    fake_zerobus_stop(server);
}

int main(void)
{
    if (fake_zerobus_start(NULL, &service) != ZEROBUS_STATUS_OK) {
        fprintf(stderr, "cannot start the fake Zerobus service\n");
        return 1;
    }
    test_stream_builder_validation();
    test_stream_builder_edges();
    test_stream_oauth_transactional();
    test_ingest_validation();
    test_flush_close_idempotent();
    test_stream_free_null_safe();
    test_stream_builder_retains_sdk();
    test_stream_retains_sdk();
    test_concurrent_sdk_child_ownership();
    test_concurrent_ingest_flush_close();
    test_stream_out_error_must_be_null();
    test_stream_out_handle_untouched_on_failure();
    test_flush_acknowledges_records();
    test_acked_offset_without_waiting();
    test_records_span_buffers();
    test_build_reports_refusal();
    test_headers_provider_authorizes();
    test_recovery_resends_unacknowledged();
    test_failure_without_recovery();
    fake_zerobus_stop(service);
    TEST_MAIN_RETURN();
}
