#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

#include "error.h"
#include "internal/auth/headers_provider.h"
#include "internal/common/utils.h"
#include "internal/ephemeral_stream/ephemeral_stream.h"
#include "internal/platform/log.h"
#include "sdk.h"
#include "zerobus/sdk.h"
#include "zerobus/stream.h"

/* Record size limit: just under the server's ~10 MiB request cap, leaving
 * headroom for framing and protocol overhead. No public knob for it yet. */
#define ZB_MAX_PAYLOAD_BYTES (10u * 1024u * 1024u - 64u * 1024u)

#define DEFAULT_MAX_INFLIGHT ((size_t)1 << 20)
#define DEFAULT_FLUSH_TIMEOUT_MS UINT64_C(300000)
/* Recovery settings, as in the Rust SDK. */
#define RECOVERY_RETRIES 4u
#define RECOVERY_TIMEOUT_MS UINT64_C(15000)
#define RECOVERY_BACKOFF_MS UINT64_C(2000)
#define LACK_OF_ACK_TIMEOUT_MS UINT64_C(60000)

struct zerobus_stream_builder {
    zerobus_sdk_t *sdk; /* owned reference */
    char *table;
    char *client_id;
    char *client_secret;                 /* zeroized on free */
    zerobus_headers_provider_t *headers; /* owned reference, or NULL */
    size_t max_inflight;
    bool recovery;
    uint64_t flush_timeout_ms;
};

struct zerobus_stream {
    zerobus_sdk_t *sdk; /* owned reference */
    zb_ephemeral_stream_t *core;
    atomic_bool closed; /* set by close, rejects ingest once true */
};

/* ---- builder ----------------------------------------------------------- */

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
    b->max_inflight = DEFAULT_MAX_INFLIGHT;
    b->recovery = true;
    b->flush_timeout_ms = DEFAULT_FLUSH_TIMEOUT_MS;
    *out_builder = b;
    ZB_DEBUG("stream builder created");
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_stream_builder_set_table(zerobus_stream_builder_t *builder,
                                 zerobus_string_view_t table_name,
                                 zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
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
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
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
    free(builder->client_id);
    zb_secure_free_cstr(builder->client_secret);
    builder->client_id = id_copy;
    builder->client_secret = secret_copy;
    zerobus_headers_provider_free(builder->headers);
    builder->headers = NULL;
    return ZEROBUS_STATUS_OK;
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
    zb_headers_provider_ref(provider);
    zerobus_headers_provider_free(builder->headers);
    builder->headers = provider;
    free(builder->client_id);
    zb_secure_free_cstr(builder->client_secret);
    builder->client_id = NULL;
    builder->client_secret = NULL;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_stream_builder_set_max_inflight_records(
    zerobus_stream_builder_t *builder, size_t max_records,
    zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL || max_records == 0 ||
        max_records > (size_t)ZB_SEM_VALUE_MAX / 2) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "max in-flight records must be between 1 and %u",
                       (unsigned)ZB_SEM_VALUE_MAX / 2);
    }
    size_t capacity = 4;
    while (capacity < max_records) {
        capacity *= 2;
    }
    builder->max_inflight = capacity;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_stream_builder_set_recovery(zerobus_stream_builder_t *builder,
                                    bool enabled, zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
    }
    builder->recovery = enabled;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_stream_builder_set_flush_timeout_ms(zerobus_stream_builder_t *builder,
                                            uint64_t timeout_ms,
                                            zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
    }
    builder->flush_timeout_ms = timeout_ms;
    return ZEROBUS_STATUS_OK;
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
    if (builder->headers == NULL &&
        (builder->client_id == NULL || builder->client_secret == NULL)) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "OAuth client credentials or a headers provider are "
                       "required");
    }

    zerobus_stream_t *s = (zerobus_stream_t *)calloc(1, sizeof(*s));
    if (s == NULL) {
        ZB_ERROR("stream allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    atomic_init(&s->closed, false);
    s->sdk = builder->sdk;
    zb_sdk_ref(s->sdk);

    zb_sdk_endpoint_t endpoint;
    zb_sdk_endpoint(s->sdk, &endpoint);
    zerobus_headers_provider_t *headers = builder->headers;
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    if (headers == NULL) {
        status = zb_oauth_headers_provider_new(
            endpoint.uc_endpoint, endpoint.workspace_id,
            zerobus_string_view(builder->table),
            zerobus_string_view(builder->client_id),
            zerobus_string_view(builder->client_secret), &headers);
    } else {
        zb_headers_provider_ref(headers);
    }
    if (status != ZEROBUS_STATUS_OK) {
        zerobus_stream_free(s);
        return status;
    }
    const zb_ephemeral_stream_config_t config = {
        .table = builder->table,
        .host = endpoint.host,
        .port = endpoint.port,
        .plaintext = endpoint.plaintext,
        .tls_context = endpoint.tls_context,
        .headers = headers,
        .max_inflight = builder->max_inflight,
        .recovery = builder->recovery,
        .recovery_retries = RECOVERY_RETRIES,
        .recovery_timeout_ms = RECOVERY_TIMEOUT_MS,
        .recovery_backoff_ms = RECOVERY_BACKOFF_MS,
        .lack_of_ack_timeout_ms = LACK_OF_ACK_TIMEOUT_MS,
        .flush_timeout_ms = builder->flush_timeout_ms};
    status = zb_ephemeral_stream_new(&config, &s->core, out_error);
    zerobus_headers_provider_free(headers);
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
    free(builder->client_id);
    zb_secure_free_cstr(builder->client_secret);
    zerobus_headers_provider_free(builder->headers);
    zerobus_sdk_free(builder->sdk);
    free(builder);
    ZB_DEBUG("stream builder freed");
}

/* ---- stream ------------------------------------------------------------ */

/* Checks one record of an ingest call. */
static zerobus_status_t check_record(zerobus_string_view_t json_record,
                                     zerobus_error_t **out_error)
{
    if (zb_is_empty(json_record)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "record must be non-empty");
    }
    if (json_record.len > ZB_MAX_PAYLOAD_BYTES) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "record of %zu bytes exceeds the %u-byte limit",
                       json_record.len, (unsigned)ZB_MAX_PAYLOAD_BYTES);
    }
    if (!zb_is_valid_string(json_record)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "record must be valid UTF-8 without embedded NUL");
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
    zerobus_status_t status = check_record(json_record, out_error);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    return zb_ephemeral_stream_ingest(stream->core, &json_record, 1, out_offset,
                                      out_error);
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
    size_t total = 0;
    for (size_t i = 0; i < record_count; ++i) {
        zerobus_status_t status = check_record(json_records[i], out_error);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        total += json_records[i].len + 16;
        if (total > ZB_MAX_PAYLOAD_BYTES) {
            return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                           "batch exceeds the %u-byte limit",
                           (unsigned)ZB_MAX_PAYLOAD_BYTES);
        }
    }
    if (record_count == 0) {
        if (out_offset != NULL) {
            *out_offset = -1;
        }
        return ZEROBUS_STATUS_OK;
    }
    return zb_ephemeral_stream_ingest(stream->core, json_records, record_count,
                                      out_offset, out_error);
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
    return zb_ephemeral_stream_flush(stream->core, out_error);
}

zerobus_status_t zerobus_stream_get_acked_offset(zerobus_stream_t *stream,
                                                 zerobus_offset_t *out_offset,
                                                 zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (stream == NULL || out_offset == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "stream and out_offset must not be NULL");
    }
    return zb_ephemeral_stream_acked_offset(stream->core, out_offset,
                                            out_error);
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
    atomic_store(&stream->closed, true);
    zerobus_status_t status =
        zb_ephemeral_stream_close(stream->core, out_error);
    ZB_DEBUG("stream closed");
    return status;
}

void zerobus_stream_free(zerobus_stream_t *stream)
{
    if (stream == NULL) {
        return;
    }
    zb_ephemeral_stream_free(stream->core);
    zerobus_sdk_free(stream->sdk);
    free(stream);
    ZB_DEBUG("stream freed");
}
