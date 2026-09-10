#include <limits.h>
#include <stdlib.h>

#include "error.h"
#include "utils.h"
#include "zerobus/sdk.h"

struct zerobus_sdk_builder {
    char *endpoint;    /* zerobus endpoint, owned */
    char *uc_endpoint; /* unity catalog endpoint, owned */
};

struct zerobus_sdk {
    /* Independent copies of the validated configuration. */
    char *endpoint;
    char *uc_endpoint;
};

/* ---- builder ----------------------------------------------------------- */

zerobus_status_t zerobus_sdk_builder_new(zerobus_sdk_builder_t **out_builder,
                                         zerobus_error_t **out_error)
{
    if (out_builder != NULL) {
        *out_builder = NULL;
    }
    if (out_error != NULL && *out_error != NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (out_builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "out_builder must not be NULL");
    }
    zerobus_sdk_builder_t *b = (zerobus_sdk_builder_t *)calloc(1, sizeof(*b));
    if (b == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    *out_builder = b;
    return ZEROBUS_STATUS_OK;
}

/* Accepts a bare http(s)://host[:port] origin for both endpoints; https is the
 * norm, http is for a local plaintext server. */
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
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_sdk_builder_build(const zerobus_sdk_builder_t *builder,
                                           zerobus_sdk_t **out_sdk,
                                           zerobus_error_t **out_error)
{
    if (out_sdk != NULL) {
        *out_sdk = NULL;
    }
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

    /* Endpoints were validated when set.
     * TODO: initialize shared TLS/OAuth/transport state. */
    zerobus_sdk_t *sdk = (zerobus_sdk_t *)calloc(1, sizeof(*sdk));
    if (sdk == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    sdk->endpoint = zb_strdup(builder->endpoint);
    sdk->uc_endpoint = zb_strdup(builder->uc_endpoint);
    if (sdk->endpoint == NULL || sdk->uc_endpoint == NULL) {
        zerobus_sdk_free(sdk);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }

    *out_sdk = sdk;
    return ZEROBUS_STATUS_OK;
}

void zerobus_sdk_builder_free(zerobus_sdk_builder_t *builder)
{
    if (builder == NULL) {
        return;
    }
    free(builder->endpoint);
    free(builder->uc_endpoint);
    free(builder);
}

/* ---- SDK --------------------------------------------------------------- */

void zerobus_sdk_free(zerobus_sdk_t *sdk)
{
    if (sdk == NULL) {
        return;
    }
    /* TODO: tear down shared transport/auth resources (best-effort). */
    free(sdk->endpoint);
    free(sdk->uc_endpoint);
    free(sdk);
}
