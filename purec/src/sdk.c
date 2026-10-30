#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "error.h"
#include "internal/common/utils.h"
#include "internal/platform/log.h"
#include "sdk.h"
#include "zerobus/sdk.h"

#define DEFAULT_TOKEN_REFRESH_BUFFER_MS 300000
/* ZEROBUS_VERSION comes from the CMake project version. */
#define SDK_IDENTIFIER "zerobus-sdk-c/" ZEROBUS_VERSION

struct zerobus_sdk_builder {
    char *endpoint;         /* zerobus endpoint, owned */
    char *uc_endpoint;      /* unity catalog endpoint, owned */
    char *application_name; /* owned, NULL when unset */
    uint64_t token_refresh_buffer_ms;
    bool connection_per_stream;
};

/* ---- builder ----------------------------------------------------------- */

zerobus_status_t zerobus_sdk_builder_new(zerobus_sdk_builder_t **out_builder,
                                         zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (out_builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "out_builder must not be NULL");
    }
    zerobus_sdk_builder_t *b = (zerobus_sdk_builder_t *)calloc(1, sizeof(*b));
    if (b == NULL) {
        ZB_ERROR("SDK builder allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    b->token_refresh_buffer_ms = DEFAULT_TOKEN_REFRESH_BUFFER_MS;
    b->connection_per_stream = true;
    *out_builder = b;
    ZB_DEBUG("SDK builder created");
    return ZEROBUS_STATUS_OK;
}

/* Argument checks shared by the builder setters. */
static zerobus_status_t check_setter_args(const zerobus_sdk_builder_t *builder,
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

/* Accepts a bare http(s)://host[:port] origin for both endpoints */
static bool validate_endpoint(zerobus_string_view_t endpoint, const char *label,
                              zerobus_error_t **out_error)
{
    const char *fmt;
    switch (zb_url_validate(endpoint)) {
    case ZB_URL_OK:
        return true;
    case ZB_URL_NO_SCHEME:
        fmt = "%s must start with http:// or https://: %.*s";
        break;
    case ZB_URL_EMPTY_HOST:
        fmt = "%s has no host: %.*s";
        break;
    case ZB_URL_BAD_HOST:
        fmt = "%s host is malformed: %.*s";
        break;
    case ZB_URL_BAD_PORT:
        fmt = "%s has an invalid port: %.*s";
        break;
    case ZB_URL_HAS_PATH:
    default:
        fmt = "%s must be a bare scheme://host[:port] with no path, query, "
              "fragment, or trailing slash: %.*s";
        break;
    }
    if (out_error != NULL) {
        int shown = endpoint.len > INT_MAX ? INT_MAX : (int)endpoint.len;
        *out_error = zb_error_newf(ZEROBUS_STATUS_INVALID_ARGUMENT, fmt, label,
                                   shown, endpoint.data);
    }
    return false;
}

zerobus_status_t
zerobus_sdk_builder_set_endpoint(zerobus_sdk_builder_t *builder,
                                 zerobus_string_view_t zerobus_endpoint,
                                 zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (!zb_is_valid_string(zerobus_endpoint)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "zerobus endpoint must be non-empty valid UTF-8");
    }
    if (!validate_endpoint(zerobus_endpoint, "zerobus endpoint", out_error)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (!zb_replace_string(&builder->endpoint, zerobus_endpoint)) {
        ZB_ERROR("SDK endpoint allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_sdk_builder_set_unity_catalog_endpoint(
    zerobus_sdk_builder_t *builder,
    zerobus_string_view_t unity_catalog_endpoint, zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (!zb_is_valid_string(unity_catalog_endpoint)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "unity catalog endpoint must be non-empty valid UTF-8");
    }
    if (!validate_endpoint(unity_catalog_endpoint, "unity catalog endpoint",
                           out_error)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (!zb_replace_string(&builder->uc_endpoint, unity_catalog_endpoint)) {
        ZB_ERROR("SDK Unity Catalog endpoint allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_sdk_builder_set_application_name(zerobus_sdk_builder_t *builder,
                                         zerobus_string_view_t application_name,
                                         zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    zerobus_string_view_t name = zb_trim_whitespace(application_name);
    if (name.len == 0) {
        free(builder->application_name);
        builder->application_name = NULL;
        return ZEROBUS_STATUS_OK;
    }
    if (!zb_is_header_value(name)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "application name must be printable ASCII");
    }
    if (!zb_replace_string(&builder->application_name, name)) {
        ZB_ERROR("SDK application name allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_sdk_builder_set_token_refresh_buffer_ms(zerobus_sdk_builder_t *builder,
                                                uint64_t buffer_ms,
                                                zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status == ZEROBUS_STATUS_OK) {
        builder->token_refresh_buffer_ms = buffer_ms;
    }
    return status;
}

zerobus_status_t zerobus_sdk_builder_set_connection_per_stream(
    zerobus_sdk_builder_t *builder, bool enabled, zerobus_error_t **out_error)
{
    zerobus_status_t status = check_setter_args(builder, out_error);
    if (status == ZEROBUS_STATUS_OK) {
        builder->connection_per_stream = enabled;
    }
    return status;
}

zerobus_status_t zerobus_sdk_builder_build(const zerobus_sdk_builder_t *builder,
                                           zerobus_sdk_t **out_sdk,
                                           zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL || out_sdk == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder and out_sdk must not be NULL");
    }
    if (builder->endpoint == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "zerobus endpoint is required");
    }
    if (builder->uc_endpoint == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "unity catalog endpoint is required");
    }

    /* Endpoints and options were validated when set.
     * TODO: shared transport state. */
    zerobus_sdk_t *sdk = (zerobus_sdk_t *)calloc(1, sizeof(*sdk));
    if (sdk == NULL) {
        ZB_ERROR("SDK allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    atomic_init(&sdk->ref_cnt, 1);
    sdk->endpoint = zb_strdup(builder->endpoint);
    sdk->uc_endpoint = zb_strdup(builder->uc_endpoint);
    sdk->application_name = zb_strdup(builder->application_name);
    sdk->token_refresh_buffer_ms = builder->token_refresh_buffer_ms;
    sdk->connection_per_stream = builder->connection_per_stream;
    const char *host = strstr(builder->endpoint, "://") + 3;
    sdk->workspace_id = zb_strndup(host, strcspn(host, ".:"));
    /* The endpoint has no path and no IPv6 literal, so any ':' after the
     * host starts a valid port. */
    size_t host_len = strcspn(host, ":");
    sdk->host = zb_strndup(host, host_len);
    sdk->plaintext = !zb_starts_with(builder->endpoint,
                                     strlen(builder->endpoint), "https://");
    uint32_t port = sdk->plaintext ? 80 : 443;
    if (host[host_len] == ':') {
        (void)zb_parse_u32((const uint8_t *)host + host_len + 1,
                           strlen(host + host_len + 1), &port);
    }
    sdk->port = (uint16_t)port;
    size_t agent_len =
        sizeof(SDK_IDENTIFIER) + (builder->application_name != NULL
                                      ? 1 + strlen(builder->application_name)
                                      : 0);
    sdk->user_agent = (char *)malloc(agent_len);
    if (sdk->user_agent != NULL) {
        (void)snprintf(
            sdk->user_agent, agent_len, "%s%s%s", SDK_IDENTIFIER,
            builder->application_name != NULL ? " " : "",
            builder->application_name != NULL ? builder->application_name : "");
    }
    if (sdk->endpoint == NULL || sdk->uc_endpoint == NULL ||
        sdk->workspace_id == NULL ||
        (builder->application_name != NULL && sdk->application_name == NULL) ||
        sdk->host == NULL || sdk->user_agent == NULL) {
        ZB_ERROR("SDK configuration allocation failed");
        zerobus_sdk_free(sdk);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    if (zb_starts_with(sdk->uc_endpoint, strlen(sdk->uc_endpoint),
                       "https://")) {
        zerobus_status_t status = zb_tls_ctx_new(NULL, &sdk->uc_tls_context);
        if (status != ZEROBUS_STATUS_OK) {
            zerobus_sdk_free(sdk);
            return zb_fail(out_error, status,
                           "could not load the system's trusted certificates");
        }
    }
    if (!sdk->plaintext) {
        zerobus_status_t status = zb_tls_ctx_new(NULL, &sdk->tls_context);
        if (status != ZEROBUS_STATUS_OK) {
            zerobus_sdk_free(sdk);
            return zb_fail(out_error, status,
                           "could not load the system's trusted certificates");
        }
    }

    *out_sdk = sdk;
    ZB_DEBUG("SDK created");
    return ZEROBUS_STATUS_OK;
}

void zerobus_sdk_builder_free(zerobus_sdk_builder_t *builder)
{
    if (builder == NULL) {
        return;
    }
    free(builder->endpoint);
    free(builder->uc_endpoint);
    free(builder->application_name);
    free(builder);
    ZB_DEBUG("SDK builder freed");
}

/* ---- SDK --------------------------------------------------------------- */

void zb_sdk_ref(zerobus_sdk_t *sdk)
{
    atomic_fetch_add(&sdk->ref_cnt, 1);
}

void zerobus_sdk_free(zerobus_sdk_t *sdk)
{
    if (sdk == NULL) {
        return;
    }
    if (atomic_fetch_sub(&sdk->ref_cnt, 1) != 1) {
        return;
    }
    /* TODO: tear down shared transport/auth resources (best-effort). */
    free(sdk->endpoint);
    free(sdk->uc_endpoint);
    free(sdk->application_name);
    free(sdk->workspace_id);
    zb_tls_ctx_free(sdk->uc_tls_context);
    free(sdk->host);
    zb_tls_ctx_free(sdk->tls_context);
    free(sdk->user_agent);
    free(sdk);
    ZB_DEBUG("SDK freed");
}
