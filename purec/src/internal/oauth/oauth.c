#include "oauth.h"

#include <ctype.h>
#include <jansson.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "error.h"
#include "headers_provider.h"
#include "internal/common/utils.h"
#include "internal/network/http_client.h"
#include "internal/platform/log.h"

#define TOKEN_PATH "/oidc/v1/token"

/* Longest response body quoted in the error for a failed token request. */
#define MAX_ERROR_BODY 512u

/* ---- token ------------------------------------------------------------- */

void zb_fetched_token_clear(zb_fetched_token_t *token)
{
    zb_secure_free_cstr(token->token);
    *token = (zb_fetched_token_t){0};
}

/* ---- helpers ----------------------------------------------------------- */

/* Returns a new printf-formatted string, or NULL on failure. */
static char *format(const char *fmt, ...)
#if defined(__GNUC__)
    __attribute__((format(printf, 1, 2)))
#endif
    ;

static char *format(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int len = vsnprintf(NULL, 0, fmt, args);
    va_end(args);
    if (len < 0) {
        return NULL;
    }
    char *text = (char *)malloc((size_t)len + 1);
    if (text == NULL) {
        return NULL;
    }
    va_start(args, fmt);
    vsnprintf(text, (size_t)len + 1, fmt, args);
    va_end(args);
    return text;
}

static char *base64_encode(const unsigned char *data, size_t len)
{
    static const char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    char *text = (char *)malloc(4 * ((len + 2) / 3) + 1);
    if (text == NULL) {
        return NULL;
    }
    char *out = text;
    for (size_t i = 0; i < len; i += 3) {
        uint32_t bits = (uint32_t)data[i] << 16;
        if (i + 1 < len) {
            bits |= (uint32_t)data[i + 1] << 8;
        }
        if (i + 2 < len) {
            bits |= data[i + 2];
        }
        *out++ = alphabet[(bits >> 18) & 63];
        *out++ = alphabet[(bits >> 12) & 63];
        *out++ = i + 1 < len ? alphabet[(bits >> 6) & 63] : '=';
        *out++ = i + 2 < len ? alphabet[bits & 63] : '=';
    }
    *out = '\0';
    return text;
}

static bool is_unreserved(unsigned char c)
{
    return isalnum(c) || c == '-' || c == '.' || c == '_' || c == '~';
}

static char *form_encode(const char *text)
{
    static const char hex[] = "0123456789ABCDEF";
    size_t len = 0;
    for (const char *c = text; *c != '\0'; ++c) {
        len += is_unreserved((unsigned char)*c) ? 1 : 3;
    }
    char *encoded = (char *)malloc(len + 1);
    if (encoded == NULL) {
        return NULL;
    }
    char *out = encoded;
    for (const unsigned char *c = (const unsigned char *)text; *c != '\0';
         ++c) {
        if (is_unreserved(*c)) {
            *out++ = (char)*c;
        } else {
            *out++ = '%';
            *out++ = hex[*c >> 4];
            *out++ = hex[*c & 15];
        }
    }
    *out = '\0';
    return encoded;
}

/* ---- request ----------------------------------------------------------- */

/* Returns len bytes of text as a JSON string, quoted and escaped, or NULL on
 * OOM. */
static char *json_quote(const char *text, size_t len)
{
    json_t *string = json_stringn(text, len);
    char *quoted = string != NULL ? json_dumps(string, JSON_ENCODE_ANY) : NULL;
    json_decref(string);
    return quoted;
}

#define AUTHORIZATION_DETAILS                                                  \
    "[\n"                                                                      \
    "  {\n"                                                                    \
    "    \"type\": \"unity_catalog_privileges\",\n"                            \
    "    \"privileges\": [\"USE CATALOG\"],\n"                                 \
    "    \"object_type\": \"CATALOG\",\n"                                      \
    "    \"object_full_path\": %s\n"                                           \
    "  },\n"                                                                   \
    "  {\n"                                                                    \
    "    \"type\": \"unity_catalog_privileges\",\n"                            \
    "    \"privileges\": [\"USE SCHEMA\"],\n"                                  \
    "    \"object_type\": \"SCHEMA\",\n"                                       \
    "    \"object_full_path\": %s\n"                                           \
    "  },\n"                                                                   \
    "  {\n"                                                                    \
    "    \"type\": \"unity_catalog_privileges\",\n"                            \
    "    \"privileges\": [\"SELECT\", \"MODIFY\"],\n"                          \
    "    \"object_type\": \"TABLE\",\n"                                        \
    "    \"object_full_path\": %s,\n"                                          \
    "    \"operations\": [\"zerobuswrite\"]\n"                                 \
    "  }\n"                                                                    \
    "]"

static char *authorization_details(const char *table)
{
    const char *schema_dot = strchr(table, '.');
    const char *table_dot = strchr(schema_dot + 1, '.');
    char *catalog = json_quote(table, (size_t)(schema_dot - table));
    char *schema = json_quote(table, (size_t)(table_dot - table));
    char *full_name = json_quote(table, strlen(table));
    char *text = catalog != NULL && schema != NULL && full_name != NULL
                     ? format(AUTHORIZATION_DETAILS, catalog, schema, full_name)
                     : NULL;
    free(catalog);
    free(schema);
    free(full_name);
    return text;
}

static bool add_field(char **form, const char *name, const char *value)
{
    char *encoded = form_encode(value);
    char *grown = encoded != NULL ? format("%s%s%s=%s", *form ? *form : "",
                                           *form ? "&" : "", name, encoded)
                                  : NULL;
    free(encoded);
    free(*form);
    *form = grown;
    return grown != NULL;
}

static bool add_header(zerobus_headers_t *headers, const char *name,
                       const char *value)
{
    return zerobus_headers_add(headers, zerobus_string_view(name),
                               zerobus_string_view(value),
                               NULL) == ZEROBUS_STATUS_OK;
}

/* Builds the headers and the form body of the token request. Fails only with
 * OUT_OF_MEMORY. */
static zerobus_status_t build_request(const char *workspace_id,
                                      const char *table, const char *client_id,
                                      const char *client_secret,
                                      zerobus_headers_t *headers,
                                      char **out_body)
{
    char *credentials = format("%s:%s", client_id, client_secret);
    char *encoded = credentials != NULL
                        ? base64_encode((const unsigned char *)credentials,
                                        strlen(credentials))
                        : NULL;
    char *authorization = encoded != NULL ? format("Basic %s", encoded) : NULL;
    char *resource = format(
        "api://databricks/workspaces/%s/zerobusDirectWriteApi", workspace_id);
    char *details = authorization_details(table);
    char *body = NULL;
    bool ok = authorization != NULL && resource != NULL && details != NULL &&
              add_field(&body, "scope", "all-apis") &&
              add_field(&body, "resource", resource) &&
              add_field(&body, "authorization_details", details) &&
              add_field(&body, "grant_type", "client_credentials") &&
              add_header(headers, "Authorization", authorization);
    zb_secure_free_cstr(credentials);
    zb_secure_free_cstr(encoded);
    zb_secure_free_cstr(authorization);
    free(resource);
    free(details);
    if (!ok) {
        ZB_ERROR("OAuth token request allocation failed");
        free(body);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    *out_body = body;
    return ZEROBUS_STATUS_OK;
}

/* ---- response ---------------------------------------------------------- */

static uint64_t parse_expires_in(const json_t *value)
{
    if (json_is_integer(value)) {
        json_int_t seconds = json_integer_value(value);
        return seconds > 0 ? (uint64_t)seconds : 0;
    }
    zerobus_string_view_t text = zb_trim_whitespace((zerobus_string_view_t){
        json_string_value(value), json_string_length(value)});
    uint64_t seconds = 0;
    return zb_parse_u64((const uint8_t *)text.data, text.len, &seconds)
               ? seconds
               : 0;
}

/* Parses the JSON body of a 2xx token response into *out_token. INTERNAL for
 * a body without an access_token that is a non-empty header value, leaving
 * *out_token untouched. */
static zerobus_status_t parse_response(const char *body, size_t len,
                                       zb_fetched_token_t *out_token,
                                       zerobus_error_t **out_error)
{
    json_t *root = json_loadb(body, len, 0, NULL);
    if (root == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INTERNAL,
                       "token response is not valid JSON");
    }
    const json_t *access_token = json_object_get(root, "access_token");
    zerobus_string_view_t token = {json_string_value(access_token),
                                   json_string_length(access_token)};
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    /* Checked so a token that cannot be sent never replaces one that can. */
    if (!zb_is_valid_string(token) || !zb_is_header_value(token)) {
        status = zb_fail(out_error, ZEROBUS_STATUS_INTERNAL,
                         "token response has no access_token that can be "
                         "sent as a header value");
    } else {
        char *copy = zb_strdup_view(token);
        if (copy == NULL) {
            ZB_ERROR("OAuth token allocation failed");
            status = ZEROBUS_STATUS_OUT_OF_MEMORY;
        } else {
            out_token->token = copy;
            out_token->expires_in =
                parse_expires_in(json_object_get(root, "expires_in"));
        }
    }
    json_decref(root);
    return status;
}

/* Fails for a non-2xx response: UNAVAILABLE for 5xx, which may succeed on
 * retry, and UNAUTHENTICATED otherwise. The error quotes the body if it is
 * short text. */
static zerobus_status_t http_error(const zb_http_response_t *response,
                                   zerobus_error_t **out_error)
{
    zerobus_string_view_t reason = {response->body, response->body_len};
    if (reason.len > MAX_ERROR_BODY || !zb_is_valid_string(reason)) {
        reason = zerobus_string_view("");
    }
    zerobus_status_t status = response->status >= 500
                                  ? ZEROBUS_STATUS_UNAVAILABLE
                                  : ZEROBUS_STATUS_UNAUTHENTICATED;
    return zb_fail(out_error, status,
                   "token request failed with HTTP status %u%s%.*s",
                   (unsigned)response->status, reason.len ? ": " : "",
                   (int)reason.len, reason.data);
}

/* ---- fetch ------------------------------------------------------------- */

zerobus_status_t
zb_oauth_fetch_token(const zb_tls_ctx_t *tls, const char *uc_endpoint,
                     const char *workspace_id, const char *table,
                     const char *client_id, const char *client_secret,
                     zb_deadline_t deadline, zb_fetched_token_t *out_token,
                     zerobus_error_t **out_error)
{
    zerobus_headers_t headers = {0};
    zb_http_response_t response = {0};
    char *body = NULL;
    zerobus_status_t status = build_request(workspace_id, table, client_id,
                                            client_secret, &headers, &body);
    if (status != ZEROBUS_STATUS_OK) {
        goto done;
    }
    status = zb_http_client_post(tls, uc_endpoint, TOKEN_PATH, &headers, body,
                                 strlen(body), deadline, &response);
    if (status != ZEROBUS_STATUS_OK) {
        status =
            zb_fail(out_error, status,
                    "token request to %s" TOKEN_PATH " failed", uc_endpoint);
    } else if (response.status / 100 != 2) {
        status = http_error(&response, out_error);
    } else {
        status = parse_response(response.body, response.body_len, out_token,
                                out_error);
    }
done:
    zb_http_response_clear(&response);
    zb_headers_clear(&headers);
    free(body);
    return status;
}
