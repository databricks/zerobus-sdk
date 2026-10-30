#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

#include "error.h"
#include "headers_provider.h"
#include "internal/common/utils.h"
#include "internal/platform/log.h"
#include "internal/proto/zerobus_service.h"
#include "internal/stream/workers.h"
#include "sdk.h"
#include "stream.h"
#include "zerobus/error.h"
#include "zerobus/sdk.h"
#include "zerobus/stream.h"

struct zerobus_stream_builder {
    zerobus_sdk_t *sdk; /* owned reference */
    char *table;
    zb_stream_config_t config;

    enum { AUTH_NONE, AUTH_OAUTH, AUTH_HEADERS_PROVIDER } auth_kind;
    union {
        struct {
            char *client_id;
            char *client_secret; /* zeroized on free */
        } oauth;
        zerobus_headers_provider_t *headers_provider; /* owned reference */
    } auth;
};

/* ---- builder ----------------------------------------------------------- */

static void clear_auth(zerobus_stream_builder_t *builder)
{
    switch (builder->auth_kind) {
    case AUTH_NONE:
        break;
    case AUTH_OAUTH:
        free(builder->auth.oauth.client_id);
        zb_secure_free_cstr(builder->auth.oauth.client_secret);
        break;
    case AUTH_HEADERS_PROVIDER:
        zerobus_headers_provider_free(builder->auth.headers_provider);
        break;
    }
    builder->auth_kind = AUTH_NONE;
}

zerobus_status_t
zerobus_stream_builder_new(zerobus_sdk_t *sdk,
                           zerobus_stream_builder_t **out_builder,
                           zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (sdk == NULL || out_builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "sdk and out_builder must not be NULL");
    }
    zerobus_stream_builder_t *b =
        (zerobus_stream_builder_t *)calloc(1, sizeof(*b));
    if (b == NULL) {
        ZB_ERROR("stream builder allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    b->sdk = sdk;
    zb_sdk_ref(sdk);
    b->config = zb_stream_config_default;
    *out_builder = b;
    ZB_DEBUG("stream builder created");
    return ZEROBUS_STATUS_OK;
}

/* Argument checks shared by the builder setters. */
static zerobus_status_t
check_setter_args(const zerobus_stream_builder_t *builder,
                  zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_stream_builder_set_table(zerobus_stream_builder_t *builder,
                                 zerobus_string_view_t table_name,
                                 zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (!zb_is_valid_string(table_name)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "table name must be non-empty valid UTF-8");
    }
    if (!zb_table_name_is_valid(table_name)) {
        return zb_fail(
            out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
            "table name must be fully qualified as catalog.schema.table");
    }
    if (!zb_replace_string(&builder->table, table_name)) {
        ZB_ERROR("stream table allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_stream_builder_set_oauth(
    zerobus_stream_builder_t *builder, zerobus_string_view_t client_id,
    zerobus_string_view_t client_secret, zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (!zb_is_valid_string(client_id) || !zb_is_valid_string(client_secret)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "client id and secret must be non-empty valid UTF-8");
    }
    /* Allocate both copies before mutating the builder (transactional). */
    char *id_copy = zb_strdup_view(client_id);
    char *secret_copy = zb_strdup_view(client_secret);
    if (id_copy == NULL || secret_copy == NULL) {
        ZB_ERROR("stream credentials allocation failed");
        free(id_copy);
        zb_secure_free_cstr(secret_copy);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    clear_auth(builder);
    builder->auth_kind = AUTH_OAUTH;
    builder->auth.oauth.client_id = id_copy;
    builder->auth.oauth.client_secret = secret_copy;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_stream_builder_set_max_inflight_requests(
    zerobus_stream_builder_t *builder, size_t max_inflight_requests,
    zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (max_inflight_requests == 0 ||
        max_inflight_requests > ZB_MAX_INFLIGHT_REQUESTS) {
        return zb_fail(
            out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
            "max in-flight requests must be in range [1, %zu], got %zu",
            ZB_MAX_INFLIGHT_REQUESTS, max_inflight_requests);
    }
    builder->config.max_inflight_requests = max_inflight_requests;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_stream_builder_set_max_ingest_payload_bytes(
    zerobus_stream_builder_t *builder, size_t max_bytes,
    zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (max_bytes == 0 || max_bytes > ZB_MAX_ENCODED_PAYLOAD) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "max ingest payload must be in range [1, %zu], got %zu",
                       (size_t)ZB_MAX_ENCODED_PAYLOAD, max_bytes);
    }
    builder->config.max_ingest_payload_bytes = max_bytes;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_stream_builder_set_recovery(zerobus_stream_builder_t *builder,
                                    bool enabled, zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status == ZEROBUS_STATUS_OK) {
        builder->config.recovery = enabled;
    }
    return status;
}

zerobus_status_t zerobus_stream_builder_set_recovery_timeout_ms(
    zerobus_stream_builder_t *builder, uint64_t timeout_ms,
    zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status == ZEROBUS_STATUS_OK) {
        builder->config.recovery_timeout_ms = timeout_ms;
    }
    return status;
}

zerobus_status_t zerobus_stream_builder_set_recovery_backoff_ms(
    zerobus_stream_builder_t *builder, uint64_t backoff_ms,
    zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status == ZEROBUS_STATUS_OK) {
        builder->config.recovery_backoff_ms = backoff_ms;
    }
    return status;
}

zerobus_status_t
zerobus_stream_builder_set_recovery_retries(zerobus_stream_builder_t *builder,
                                            uint32_t retries,
                                            zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status == ZEROBUS_STATUS_OK) {
        builder->config.recovery_retries = retries;
    }
    return status;
}

zerobus_status_t zerobus_stream_builder_set_server_lack_of_ack_timeout_ms(
    zerobus_stream_builder_t *builder, uint64_t timeout_ms,
    zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (timeout_ms == 0) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "server lack-of-ack timeout must be non-zero");
    }
    builder->config.server_lack_of_ack_timeout_ms = timeout_ms;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_stream_builder_set_flush_timeout_ms(zerobus_stream_builder_t *builder,
                                            uint64_t timeout_ms,
                                            zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status == ZEROBUS_STATUS_OK) {
        builder->config.flush_timeout_ms = timeout_ms;
    }
    return status;
}

zerobus_status_t zerobus_stream_builder_set_stream_paused_max_wait_time_ms(
    zerobus_stream_builder_t *builder, uint64_t wait_ms,
    zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status == ZEROBUS_STATUS_OK) {
        builder->config.stream_paused_max_wait_time_ms = wait_ms;
    }
    return status;
}

zerobus_status_t zerobus_stream_builder_set_headers_provider(
    zerobus_stream_builder_t *builder, zerobus_headers_provider_t *provider,
    zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL || provider == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder and provider must not be NULL");
    }
    /* Take the new reference first, in case provider is the one already set. */
    zb_headers_provider_ref(provider);
    clear_auth(builder);
    builder->auth_kind = AUTH_HEADERS_PROVIDER;
    builder->auth.headers_provider = provider;
    return ZEROBUS_STATUS_OK;
}

static zerobus_status_t
resolve_headers_provider(const zerobus_stream_builder_t *builder,
                         const zerobus_stream_t *stream,
                         zerobus_headers_provider_t **out_provider)
{
    switch (builder->auth_kind) {
    case AUTH_OAUTH:
        return zb_oauth_headers_provider_new(
            stream, builder->auth.oauth.client_id,
            builder->auth.oauth.client_secret, out_provider);
    case AUTH_HEADERS_PROVIDER:
        zb_headers_provider_ref(builder->auth.headers_provider);
        *out_provider = builder->auth.headers_provider;
        return ZEROBUS_STATUS_OK;
    default:
        return ZEROBUS_STATUS_INTERNAL;
    }
}

/* Allocates a stream with its locks, and acked at -1. */
static zerobus_status_t new_stream(zerobus_stream_t **out_stream)
{
    zerobus_stream_t *s = (zerobus_stream_t *)calloc(1, sizeof(*s));
    if (s == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status = zb_mutex_init(&s->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        free(s);
        return status;
    }
    status = zb_cond_init(&s->changed);
    if (status == ZEROBUS_STATUS_OK) {
        status = zb_cond_init(&s->acked_changed);
        if (status != ZEROBUS_STATUS_OK) {
            (void)zb_cond_destroy(&s->changed);
        }
    }
    if (status != ZEROBUS_STATUS_OK) {
        (void)zb_mutex_destroy(&s->mutex);
        free(s);
        return status;
    }
    atomic_init(&s->closed, false);
    atomic_init(&s->acked, -1);
    atomic_init(&s->flush_waiters, 0);
    *out_stream = s;
    return ZEROBUS_STATUS_OK;
}

/* Frees the stream and everything it owns, with the requests still in the
 * landing zone. */
static void destroy(zerobus_stream_t *stream)
{
    zb_landing_zone_free(stream->zone);
    /* The OAuth provider borrows the stream, so it goes first. */
    zerobus_headers_provider_free(stream->headers_provider);
    free(stream->table);
    zerobus_sdk_free(stream->sdk);
    free(stream->failure_message);
    free(stream->close_message);
    (void)zb_cond_destroy(&stream->acked_changed);
    (void)zb_cond_destroy(&stream->changed);
    (void)zb_mutex_destroy(&stream->mutex);
    free(stream);
}

zerobus_status_t
zerobus_stream_builder_build(const zerobus_stream_builder_t *builder,
                             zerobus_stream_t **out_stream,
                             zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL || out_stream == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder and out_stream must not be NULL");
    }
    if (builder->table == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "table name is required");
    }
    if (builder->auth_kind == AUTH_NONE) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "authentication is required: set OAuth credentials "
                       "or a headers provider");
    }
    if (!builder->sdk->connection_per_stream) {
        return zb_fail(out_error, ZEROBUS_STATUS_UNIMPLEMENTED,
                       "streams sharing one connection are not implemented "
                       "yet");
    }

    zerobus_stream_t *s = NULL;
    zerobus_status_t status = new_stream(&s);
    if (status != ZEROBUS_STATUS_OK) {
        ZB_ERROR("stream allocation failed");
        return status;
    }
    s->sdk = builder->sdk;
    zb_sdk_ref(s->sdk);
    s->table = zb_strdup(builder->table);
    s->config = builder->config;
    if (s->table == NULL) {
        ZB_ERROR("stream configuration allocation failed");
        destroy(s);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    status = resolve_headers_provider(builder, s, &s->headers_provider);
    if (status != ZEROBUS_STATUS_OK) {
        destroy(s);
        return zb_fail(out_error, status, "headers provider setup failed");
    }
    status = zb_landing_zone_new(s->config.max_inflight_requests, &s->zone);
    if (status != ZEROBUS_STATUS_OK) {
        destroy(s);
        return zb_fail(out_error, status,
                       "allocating the landing zone of %zu requests failed",
                       builder->config.max_inflight_requests);
    }
    status = zb_thread_create(&s->supervisor, zb_supervisor_main, s);
    if (status != ZEROBUS_STATUS_OK) {
        destroy(s);
        return zb_fail(out_error, status, "starting the supervisor failed");
    }
    s->supervisor_running = true;

    /* The supervisor publishes the first open's result, after retrying as
     * recovery allows. */
    (void)zb_mutex_lock(&s->mutex);
    while (!s->opened) {
        (void)zb_cond_wait(&s->changed, &s->mutex);
    }
    status = s->open_status;
    if (status != ZEROBUS_STATUS_OK) {
        (void)zb_fail(out_error, status, "opening the stream failed: %s",
                      s->failure_message ? s->failure_message
                                         : "unknown error");
    }
    (void)zb_mutex_unlock(&s->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        zerobus_stream_free(s);
        return status;
    }

    *out_stream = s;
    ZB_DEBUG("stream created");
    return ZEROBUS_STATUS_OK;
}

void zerobus_stream_builder_free(zerobus_stream_builder_t *builder)
{
    if (builder == NULL) {
        return;
    }
    free(builder->table);
    clear_auth(builder);
    zerobus_sdk_free(builder->sdk);
    free(builder);
    ZB_DEBUG("stream builder freed");
}

/* ---- stream ------------------------------------------------------------ */

/* The status and message for an operation refused because the stream is
 * closing or failed. Call with mutex held. A zone closed by neither is a
 * failure being published, so this waits for it. */
static zerobus_status_t refused(zerobus_stream_t *stream,
                                zerobus_error_t **out_error)
{
    while (stream->failure == ZEROBUS_STATUS_OK &&
           !atomic_load(&stream->closed) && !stream->aborting) {
        (void)zb_cond_wait(&stream->changed, &stream->mutex);
    }
    if (stream->failure != ZEROBUS_STATUS_OK) {
        return zb_fail(out_error, stream->failure, "stream failed: %s",
                       stream->failure_message ? stream->failure_message
                                               : "unknown error");
    }
    return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                   "stream is closing or closed");
}

/* Checks one record of an ingest call. */
static zerobus_status_t check_record(const zerobus_stream_t *stream,
                                     zerobus_string_view_t json_record,
                                     zerobus_error_t **out_error)
{
    if (zb_is_empty(json_record)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "record must be non-empty");
    }
    if (json_record.len > stream->config.max_ingest_payload_bytes) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "record of %zu bytes exceeds the %zu-byte limit",
                       json_record.len,
                       stream->config.max_ingest_payload_bytes);
    }
    if (!zb_is_valid_string(json_record)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "record must be valid UTF-8 without embedded NUL");
    }
    return ZEROBUS_STATUS_OK;
}

/* Copies the count records into one request and pushes it into the landing
 * zone, waiting while it is full. Returns the request's offset. */
static zerobus_status_t admit(zerobus_stream_t *stream,
                              const zerobus_string_view_t *json_records,
                              size_t count, zerobus_offset_t *out_offset,
                              zerobus_error_t **out_error)
{
    zb_ingest_request_t *request = zb_ingest_request_new(json_records, count);
    if (request == NULL) {
        ZB_ERROR("record allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_offset_t offset = -1;
    zerobus_status_t status = zb_landing_zone_push(
        stream->zone, request, ZB_DEADLINE_INFINITE, &offset);
    if (status != ZEROBUS_STATUS_OK) {
        zb_ingest_request_free(request);
        if (status != ZEROBUS_STATUS_FAILED_PRECONDITION) {
            return zb_fail(out_error, status, "admitting the record failed");
        }
        (void)zb_mutex_lock(&stream->mutex);
        status = refused(stream, out_error);
        (void)zb_mutex_unlock(&stream->mutex);
        return status;
    }
    if (out_offset != NULL) {
        *out_offset = offset;
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_stream_ingest_json_record(
    zerobus_stream_t *stream, zerobus_string_view_t json_record,
    zerobus_offset_t *out_offset, zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (stream == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "stream must not be NULL");
    }
    if (atomic_load(&stream->closed)) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "stream is closing or closed");
    }
    zerobus_status_t status = check_record(stream, json_record, out_error);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    return admit(stream, &json_record, 1, out_offset, out_error);
}

zerobus_status_t zerobus_stream_ingest_json_records(
    zerobus_stream_t *stream, const zerobus_string_view_t *json_records,
    size_t record_count, zerobus_offset_t *out_offset,
    zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (stream == NULL || (json_records == NULL && record_count != 0)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "stream and records must not be NULL");
    }
    if (atomic_load(&stream->closed)) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "stream is closing or closed");
    }
    size_t limit = stream->config.max_ingest_payload_bytes;
    size_t total = 0;
    for (size_t i = 0; i < record_count; ++i) {
        zerobus_status_t status =
            check_record(stream, json_records[i], out_error);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        if (json_records[i].len > limit - total) {
            return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                           "batch exceeds the %zu-byte limit", limit);
        }
        total += json_records[i].len;
    }
    if (record_count == 0) {
        if (out_offset != NULL) {
            *out_offset = -1;
        }
        return ZEROBUS_STATUS_OK;
    }
    return admit(stream, json_records, record_count, out_offset, out_error);
}

/* Waits until the server acknowledges target, the stream fails, or the flush
 * timeout passes. what names the wait in the timeout's message. */
static zerobus_status_t wait_acked(zerobus_stream_t *stream,
                                   zerobus_offset_t target, const char *what,
                                   zerobus_error_t **out_error)
{
    zb_deadline_t deadline =
        zb_deadline_after_ms(stream->config.flush_timeout_ms);
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    (void)zb_mutex_lock(&stream->mutex);
    /* Published before reading acked, which the receiver writes before it
     * reads flush_waiters: one of the two sees the other. */
    (void)atomic_fetch_add(&stream->flush_waiters, 1);
    while (atomic_load(&stream->acked) < target) {
        if (stream->failure != ZEROBUS_STATUS_OK) {
            status = refused(stream, out_error);
            break;
        }
        if (zb_cond_wait_until(&stream->acked_changed, &stream->mutex,
                               deadline) == ZEROBUS_STATUS_DEADLINE_EXCEEDED &&
            atomic_load(&stream->acked) < target) {
            status = zb_fail(
                out_error, ZEROBUS_STATUS_DEADLINE_EXCEEDED,
                "%s timed out after %llu ms at offset %lld of %lld", what,
                (unsigned long long)stream->config.flush_timeout_ms,
                (long long)atomic_load(&stream->acked), (long long)target);
            break;
        }
    }
    (void)atomic_fetch_sub(&stream->flush_waiters, 1);
    (void)zb_mutex_unlock(&stream->mutex);
    return status;
}

zerobus_status_t zerobus_stream_wait_for_offset(zerobus_stream_t *stream,
                                                zerobus_offset_t offset,
                                                zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (stream == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "stream must not be NULL");
    }
    if (atomic_load(&stream->closed)) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "stream is closing or closed");
    }
    /* The last offset includes every one an ingest has returned. */
    if (offset < 0 || offset > zb_landing_zone_last_offset(stream->zone)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "no ingest on this stream returned offset %lld",
                       (long long)offset);
    }
    return wait_acked(stream, offset, "waiting for the offset", out_error);
}

zerobus_status_t zerobus_stream_flush(zerobus_stream_t *stream,
                                      zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (stream == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "stream must not be NULL");
    }
    if (atomic_load(&stream->closed)) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "stream is closing or closed");
    }
    return wait_acked(stream, zb_landing_zone_last_offset(stream->zone),
                      "flush", out_error);
}

/* Joins the supervisor once. Callers are serialized by close or free. */
static void join_supervisor(zerobus_stream_t *stream)
{
    if (stream->supervisor_running) {
        (void)zb_thread_join(&stream->supervisor, NULL);
        stream->supervisor_running = false;
    }
}

zerobus_status_t zerobus_stream_close(zerobus_stream_t *stream,
                                      zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (stream == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "stream must not be NULL");
    }
    if (atomic_exchange(&stream->closed, true)) {
        /* Another close runs or ran: report its result. */
        (void)zb_mutex_lock(&stream->mutex);
        while (!stream->close_done) {
            (void)zb_cond_wait(&stream->changed, &stream->mutex);
        }
        zerobus_status_t status = stream->close_status;
        if (status != ZEROBUS_STATUS_OK) {
            (void)zb_fail(out_error, status, "%s",
                          stream->close_message ? stream->close_message
                                                : "close failed");
        }
        (void)zb_mutex_unlock(&stream->mutex);
        return status;
    }

    (void)zb_landing_zone_close(stream->zone);
    zerobus_error_t *error = NULL;
    zerobus_status_t status = wait_acked(
        stream, zb_landing_zone_last_offset(stream->zone), "flush", &error);
    (void)zb_mutex_lock(&stream->mutex);
    if (status == ZEROBUS_STATUS_OK) {
        stream->finishing = true;
    } else {
        stream->aborting = true;
    }
    (void)zb_cond_broadcast(&stream->changed);
    (void)zb_mutex_unlock(&stream->mutex);
    join_supervisor(stream);

    ZB_DEBUG("stream to %s closed (status %u)", stream->table,
             (unsigned)status);
    zerobus_string_view_t message = zerobus_error_message(error);
    (void)zb_mutex_lock(&stream->mutex);
    stream->close_done = true;
    stream->close_status = status;
    stream->close_message =
        message.data ? zb_strndup(message.data, message.len) : NULL;
    (void)zb_cond_broadcast(&stream->changed);
    /* A later close may now return, and its caller free the stream, so the
     * stream is not touched after this. */
    (void)zb_mutex_unlock(&stream->mutex);
    if (out_error != NULL) {
        *out_error = error;
    } else {
        zerobus_error_free(error);
    }
    return status;
}

void zerobus_stream_free(zerobus_stream_t *stream)
{
    if (stream == NULL) {
        return;
    }
    (void)zb_mutex_lock(&stream->mutex);
    stream->aborting = true;
    (void)zb_cond_broadcast(&stream->changed);
    (void)zb_mutex_unlock(&stream->mutex);
    (void)zb_landing_zone_close(stream->zone);
    join_supervisor(stream);
    destroy(stream);
    ZB_DEBUG("stream freed");
}
