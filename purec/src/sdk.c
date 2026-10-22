#include <limits.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "error.h"
#include "internal/common/utils.h"
#include "internal/platform/log.h"
#include "sdk.h"
#include "zerobus/sdk.h"

struct zerobus_sdk_builder {
    char *endpoint;    /* zerobus endpoint, owned */
    char *uc_endpoint; /* unity catalog endpoint, owned */
    char *ca_file;     /* optional, owned */
};

struct zerobus_sdk {
    atomic_size_t ref_cnt;
    /* Independent copies of the validated configuration. */
    char *endpoint;
    char *uc_endpoint;
    /* Derived from the endpoint at build. */
    char *host;
    uint16_t port;
    bool plaintext;
    char *workspace_id;
    zb_tls_ctx_t *tls_context; /* NULL for plaintext. */
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
    *out_builder = b;
    ZB_DEBUG("SDK builder created");
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
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
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
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
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
zerobus_sdk_builder_set_tls_ca_file(zerobus_sdk_builder_t *builder,
                                    zerobus_string_view_t path,
                                    zerobus_error_t **out_error)
{
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
    }
    if (!zb_is_valid_string(path)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "CA file path must be non-empty valid UTF-8");
    }
    if (!zb_replace_string(&builder->ca_file, path)) {
        ZB_ERROR("SDK CA file allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    return ZEROBUS_STATUS_OK;
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

    /* Endpoints were validated when set. */
    zerobus_sdk_t *sdk = (zerobus_sdk_t *)calloc(1, sizeof(*sdk));
    if (sdk == NULL) {
        ZB_ERROR("SDK allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    atomic_init(&sdk->ref_cnt, 1);
    sdk->endpoint = zb_strdup(builder->endpoint);
    sdk->uc_endpoint = zb_strdup(builder->uc_endpoint);
    bool tls = false;
    if (sdk->endpoint == NULL || sdk->uc_endpoint == NULL ||
        !zb_url_split(sdk->endpoint, &tls, &sdk->host, &sdk->port)) {
        ZB_ERROR("SDK configuration allocation failed");
        zerobus_sdk_free(sdk);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    sdk->plaintext = !tls;
    const char *dot = strchr(sdk->host, '.');
    sdk->workspace_id = dot ? zb_strndup(sdk->host, (size_t)(dot - sdk->host))
                            : zb_strdup(sdk->host);
    if (sdk->workspace_id == NULL) {
        zerobus_sdk_free(sdk);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    if (tls) {
        zerobus_status_t status =
            zb_tls_ctx_new(builder->ca_file, &sdk->tls_context);
        if (status != ZEROBUS_STATUS_OK) {
            zerobus_sdk_free(sdk);
            return zb_fail(
                out_error, status, "loading TLS trust from %s failed",
                builder->ca_file ? builder->ca_file : "the system roots");
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
    free(builder->ca_file);
    free(builder);
    ZB_DEBUG("SDK builder freed");
}

/* ---- SDK --------------------------------------------------------------- */

void zb_sdk_ref(zerobus_sdk_t *sdk)
{
    atomic_fetch_add(&sdk->ref_cnt, 1);
}

void zb_sdk_endpoint(const zerobus_sdk_t *sdk, zb_sdk_endpoint_t *out)
{
    *out = (zb_sdk_endpoint_t){.host = sdk->host,
                               .port = sdk->port,
                               .plaintext = sdk->plaintext,
                               .tls_context = sdk->tls_context,
                               .uc_endpoint = sdk->uc_endpoint,
                               .workspace_id = sdk->workspace_id};
}

void zerobus_sdk_free(zerobus_sdk_t *sdk)
{
    if (sdk == NULL) {
        return;
    }
    if (atomic_fetch_sub(&sdk->ref_cnt, 1) != 1) {
        return;
    }
    zb_tls_ctx_free(sdk->tls_context);
    free(sdk->endpoint);
    free(sdk->uc_endpoint);
    free(sdk->host);
    free(sdk->workspace_id);
    free(sdk);
    ZB_DEBUG("SDK freed");
}
