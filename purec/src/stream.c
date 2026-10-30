#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

#include "error.h"
#include "headers_provider.h"
#include "internal/common/utils.h"
#include "internal/platform/log.h"
#include "internal/proto/zerobus_service.h"
#include "sdk.h"
#include "stream.h"
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

struct zerobus_stream {
    zerobus_sdk_t *sdk; /* owned reference */
    char *table;
    zb_stream_config_t config;
    zerobus_headers_provider_t *headers_provider; /* owned reference */

    atomic_bool closed; /* set by close, rejects ingest once true */
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
                         zerobus_headers_provider_t **out_provider)
{
    switch (builder->auth_kind) {
    case AUTH_OAUTH:
        return zb_oauth_headers_provider_new(
            builder->sdk->uc_endpoint, builder->sdk->workspace_id,
            builder->table, builder->auth.oauth.client_id,
            builder->auth.oauth.client_secret, out_provider);
    case AUTH_HEADERS_PROVIDER:
        zb_headers_provider_ref(builder->auth.headers_provider);
        *out_provider = builder->auth.headers_provider;
        return ZEROBUS_STATUS_OK;
    default:
        return ZEROBUS_STATUS_INTERNAL;
    }
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

    /* TODO: open the stream. */
    zerobus_stream_t *s = (zerobus_stream_t *)calloc(1, sizeof(*s));
    if (s == NULL) {
        ZB_ERROR("stream allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    atomic_init(&s->closed, false);
    s->sdk = builder->sdk;
    zb_sdk_ref(s->sdk);
    s->table = zb_strdup(builder->table);
    s->config = builder->config;
    if (s->table == NULL) {
        ZB_ERROR("stream configuration allocation failed");
        zerobus_stream_free(s);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status =
        resolve_headers_provider(builder, &s->headers_provider);
    if (status != ZEROBUS_STATUS_OK) {
        zerobus_stream_free(s);
        return zb_fail(out_error, status, "headers provider setup failed");
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

    /* TODO: copy the record, assign its offset, and queue it for the sender. On
     * success write the offset to *out_offset when non-NULL. Returns
     * UNIMPLEMENTED until the networking core lands. */
    (void)out_offset;
    return ZEROBUS_STATUS_UNIMPLEMENTED;
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
    /* TODO: wait for the server to acknowledge every queued record. Returns
     * UNIMPLEMENTED until the networking core lands. */
    return ZEROBUS_STATUS_UNIMPLEMENTED;
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
    /* TODO: flush pending records before teardown, returning any flush error.
     */
    atomic_store(&stream->closed, true);
    ZB_DEBUG("stream closed");
    return ZEROBUS_STATUS_OK;
}

void zerobus_stream_free(zerobus_stream_t *stream)
{
    if (stream == NULL) {
        return;
    }
    /* TODO: tear down the transport and join I/O workers before freeing. */
    free(stream->table);
    zerobus_headers_provider_free(stream->headers_provider);
    zerobus_sdk_free(stream->sdk);
    free(stream);
    ZB_DEBUG("stream freed");
}
