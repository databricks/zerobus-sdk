#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "error.h"
#include "http.h"
#include "internal/common/utils.h"
#include "internal/platform/log.h"
#include "oauth.h"

#define TOKEN_PATH "/oidc/v1/token"
#define REQUEST_TIMEOUT_MS 30000u

struct zb_oauth {
    zb_mutex_t mutex; /* Held while minting, so concurrent callers share it. */
    char *host;
    uint16_t port;
    bool tls;
    char *workspace_id;
    char *table;
    char *client_id;
    char *client_secret;       /* zeroized on free */
    zb_tls_ctx_t *tls_context; /* System roots; created on first mint. */
    char *token;               /* zeroized on free */
    zb_deadline_t refresh_at;
};

/* ---- JSON --------------------------------------------------------------- */

typedef struct {
    const char *p;
    const char *end;
} json_t;

static void skip_space(json_t *json)
{
    while (json->p < json->end && (*json->p == ' ' || *json->p == '\t' ||
                                   *json->p == '\n' || *json->p == '\r')) {
        ++json->p;
    }
}

static size_t put_utf8(char *out, uint32_t code)
{
    if (code < 0x80) {
        out[0] = (char)code;
        return 1;
    }
    if (code < 0x800) {
        out[0] = (char)(0xC0 | (code >> 6));
        out[1] = (char)(0x80 | (code & 0x3F));
        return 2;
    }
    if (code < 0x10000) {
        out[0] = (char)(0xE0 | (code >> 12));
        out[1] = (char)(0x80 | ((code >> 6) & 0x3F));
        out[2] = (char)(0x80 | (code & 0x3F));
        return 3;
    }
    out[0] = (char)(0xF0 | (code >> 18));
    out[1] = (char)(0x80 | ((code >> 12) & 0x3F));
    out[2] = (char)(0x80 | ((code >> 6) & 0x3F));
    out[3] = (char)(0x80 | (code & 0x3F));
    return 4;
}

static bool read_hex4(json_t *json, uint32_t *out)
{
    if (json->end - json->p < 4) {
        return false;
    }
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i) {
        int digit = zb_hex_value(json->p[i]);
        if (digit < 0) {
            return false;
        }
        value = value * 16 + (uint32_t)digit;
    }
    json->p += 4;
    *out = value;
    return true;
}

/* Parses the string at json->p. With out, stores a malloc-owned unescaped
 * copy; an unescaped string is never longer than its source. */
static bool parse_string(json_t *json, char **out)
{
    if (json->p >= json->end || *json->p != '"') {
        return false;
    }
    ++json->p;
    const char *start = json->p;
    char *copy = NULL, *w = NULL;
    if (out) {
        size_t limit = (size_t)(json->end - start);
        copy = malloc(limit + 1);
        if (!copy) {
            return false;
        }
        w = copy;
    }
    while (json->p < json->end && *json->p != '"') {
        char c = *json->p++;
        if ((unsigned char)c < 0x20) {
            goto fail;
        }
        if (c != '\\') {
            if (w) {
                *w++ = c;
            }
            continue;
        }
        if (json->p >= json->end) {
            goto fail;
        }
        char e = *json->p++;
        uint32_t code = 0;
        switch (e) {
        case '"':
        case '\\':
        case '/':
            code = (uint32_t)e;
            break;
        case 'b':
            code = '\b';
            break;
        case 'f':
            code = '\f';
            break;
        case 'n':
            code = '\n';
            break;
        case 'r':
            code = '\r';
            break;
        case 't':
            code = '\t';
            break;
        case 'u':
            if (!read_hex4(json, &code)) {
                goto fail;
            }
            if (code >= 0xD800 && code < 0xDC00) {
                uint32_t low = 0;
                if (json->end - json->p < 2 || json->p[0] != '\\' ||
                    json->p[1] != 'u') {
                    goto fail;
                }
                json->p += 2;
                if (!read_hex4(json, &low) || low < 0xDC00 || low > 0xDFFF) {
                    goto fail;
                }
                code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
            } else if (code >= 0xDC00 && code < 0xE000) {
                goto fail;
            }
            break;
        default:
            goto fail;
        }
        if (w) {
            w += put_utf8(w, code);
        }
    }
    if (json->p >= json->end) {
        goto fail;
    }
    ++json->p;
    if (out) {
        *w = '\0';
        *out = copy;
    }
    return true;
fail:
    free(copy);
    return false;
}

static bool skip_number(json_t *json)
{
    const char *start = json->p;
    if (json->p < json->end && *json->p == '-') {
        ++json->p;
    }
    while (json->p < json->end &&
           ((*json->p >= '0' && *json->p <= '9') || *json->p == '.' ||
            *json->p == 'e' || *json->p == 'E' || *json->p == '+' ||
            *json->p == '-')) {
        ++json->p;
    }
    return json->p != start;
}

static bool skip_literal(json_t *json, const char *literal)
{
    size_t len = strlen(literal);
    if ((size_t)(json->end - json->p) < len ||
        memcmp(json->p, literal, len) != 0) {
        return false;
    }
    json->p += len;
    return true;
}

static bool skip_value(json_t *json, unsigned depth)
{
    skip_space(json);
    if (json->p >= json->end || depth > 64) {
        return false;
    }
    char c = *json->p;
    if (c == '"') {
        return parse_string(json, NULL);
    }
    if (c == '{' || c == '[') {
        char close = c == '{' ? '}' : ']';
        ++json->p;
        skip_space(json);
        if (json->p < json->end && *json->p == close) {
            ++json->p;
            return true;
        }
        for (;;) {
            if (c == '{') {
                skip_space(json);
                if (!parse_string(json, NULL)) {
                    return false;
                }
                skip_space(json);
                if (json->p >= json->end || *json->p++ != ':') {
                    return false;
                }
            }
            if (!skip_value(json, depth + 1)) {
                return false;
            }
            skip_space(json);
            if (json->p >= json->end) {
                return false;
            }
            if (*json->p == ',') {
                ++json->p;
                continue;
            }
            if (*json->p++ != close) {
                return false;
            }
            return true;
        }
    }
    if (c == 't') {
        return skip_literal(json, "true");
    }
    if (c == 'f') {
        return skip_literal(json, "false");
    }
    if (c == 'n') {
        return skip_literal(json, "null");
    }
    return skip_number(json);
}

/* Positions *value at the value of the top-level member key. */
static bool find_member(const char *text, size_t len, const char *key,
                        json_t *value)
{
    json_t json = {text, text + len};
    skip_space(&json);
    if (json.p >= json.end || *json.p++ != '{') {
        return false;
    }
    for (;;) {
        skip_space(&json);
        if (json.p < json.end && *json.p == '}') {
            return false;
        }
        char *name = NULL;
        if (!parse_string(&json, &name)) {
            return false;
        }
        bool match = strcmp(name, key) == 0;
        free(name);
        skip_space(&json);
        if (json.p >= json.end || *json.p++ != ':') {
            return false;
        }
        skip_space(&json);
        if (match) {
            *value = json;
            return true;
        }
        if (!skip_value(&json, 0)) {
            return false;
        }
        skip_space(&json);
        if (json.p >= json.end || *json.p != ',') {
            return false;
        }
        ++json.p;
    }
}

bool zb_json_get_string(const char *text, size_t len, const char *key,
                        char **out_string)
{
    json_t value;
    return find_member(text, len, key, &value) &&
           parse_string(&value, out_string);
}

bool zb_json_get_u64(const char *text, size_t len, const char *key,
                     uint64_t *out_number)
{
    json_t value;
    if (!find_member(text, len, key, &value) || value.p >= value.end) {
        return false;
    }
    bool quoted = *value.p == '"';
    value.p += quoted;
    uint64_t number = 0;
    const char *start = value.p;
    while (value.p < value.end && *value.p >= '0' && *value.p <= '9') {
        if (number > (UINT64_MAX - 9) / 10) {
            return false;
        }
        number = number * 10 + (uint64_t)(*value.p++ - '0');
    }
    if (value.p == start ||
        (quoted && (value.p >= value.end || *value.p != '"'))) {
        return false;
    }
    *out_number = number;
    return true;
}

/* ---- request ------------------------------------------------------------ */

/* Growing NUL-terminated text buffer; a failed append latches. */
typedef struct {
    char *data;
    size_t len;
    size_t capacity;
    bool failed;
} text_t;

static void append(text_t *text, const char *data, size_t len)
{
    if (text->failed) {
        return;
    }
    if (text->len + len + 1 > text->capacity) {
        size_t capacity = text->capacity ? text->capacity : 256;
        while (capacity < text->len + len + 1) {
            capacity *= 2;
        }
        char *grown = realloc(text->data, capacity);
        if (!grown) {
            text->failed = true;
            return;
        }
        text->data = grown;
        text->capacity = capacity;
    }
    memcpy(text->data + text->len, data, len);
    text->len += len;
    text->data[text->len] = '\0';
}

static void append_str(text_t *text, const char *data)
{
    append(text, data, strlen(data));
}

/* Appends a JSON string literal of value. */
static void append_json(text_t *text, const char *value, size_t len)
{
    append(text, "\"", 1);
    for (size_t i = 0; i < len; ++i) {
        unsigned char c = (unsigned char)value[i];
        char escaped[8];
        if (c == '"' || c == '\\') {
            escaped[0] = '\\';
            escaped[1] = (char)c;
            append(text, escaped, 2);
        } else if (c < 0x20) {
            (void)snprintf(escaped, sizeof(escaped), "\\u%04x", c);
            append(text, escaped, 6);
        } else {
            append(text, (const char *)&value[i], 1);
        }
    }
    append(text, "\"", 1);
}

/* Appends value in application/x-www-form-urlencoded form. */
static void append_form(text_t *text, const char *value)
{
    static const char hex[] = "0123456789ABCDEF";
    for (const unsigned char *p = (const unsigned char *)value; *p; ++p) {
        if ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
            (*p >= '0' && *p <= '9') || *p == '-' || *p == '.' || *p == '_' ||
            *p == '~') {
            append(text, (const char *)p, 1);
        } else {
            char escaped[3] = {'%', hex[*p >> 4], hex[*p & 15]};
            append(text, escaped, 3);
        }
    }
}

static void append_base64(text_t *text, const char *data, size_t len)
{
    static const char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    const unsigned char *in = (const unsigned char *)data;
    for (size_t i = 0; i < len; i += 3) {
        uint32_t chunk = (uint32_t)in[i] << 16;
        if (i + 1 < len) {
            chunk |= (uint32_t)in[i + 1] << 8;
        }
        if (i + 2 < len) {
            chunk |= in[i + 2];
        }
        char out[4] = {alphabet[(chunk >> 18) & 63],
                       alphabet[(chunk >> 12) & 63],
                       i + 1 < len ? alphabet[(chunk >> 6) & 63] : '=',
                       i + 2 < len ? alphabet[chunk & 63] : '='};
        append(text, out, 4);
    }
}

/* The Unity Catalog privileges the token is scoped to, as in the Rust SDK. */
static void append_authorization_details(text_t *text, const char *table)
{
    const char *first = strchr(table, '.');
    const char *second = strchr(first + 1, '.');
    size_t catalog = (size_t)(first - table);
    size_t schema = (size_t)(second - table);
    append_str(text, "[{\"type\":\"unity_catalog_privileges\",\"privileges\":"
                     "[\"USE CATALOG\"],\"object_type\":\"CATALOG\","
                     "\"object_full_path\":");
    append_json(text, table, catalog);
    append_str(text, "},{\"type\":\"unity_catalog_privileges\",\"privileges\":"
                     "[\"USE SCHEMA\"],\"object_type\":\"SCHEMA\","
                     "\"object_full_path\":");
    append_json(text, table, schema);
    append_str(text, "},{\"type\":\"unity_catalog_privileges\",\"privileges\":"
                     "[\"SELECT\",\"MODIFY\"],\"object_type\":\"TABLE\","
                     "\"object_full_path\":");
    append_json(text, table, strlen(table));
    append_str(text, ",\"operations\":[\"zerobuswrite\"]}]");
}

static zerobus_status_t http_status_code(uint32_t status)
{
    switch (status) {
    case 400:
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    case 401:
        return ZEROBUS_STATUS_UNAUTHENTICATED;
    case 403:
        return ZEROBUS_STATUS_PERMISSION_DENIED;
    case 404:
        return ZEROBUS_STATUS_NOT_FOUND;
    default:
        return ZEROBUS_STATUS_UNAVAILABLE;
    }
}

static zerobus_status_t mint(zb_oauth_t *oauth, zb_deadline_t deadline,
                             uint64_t *out_expires_in,
                             zerobus_error_t **out_error)
{
    if (oauth->tls && !oauth->tls_context) {
        zerobus_status_t status = zb_tls_ctx_new(NULL, &oauth->tls_context);
        if (status != ZEROBUS_STATUS_OK) {
            return zb_fail(out_error, status,
                           "OAuth TLS setup failed: cannot load system roots");
        }
    }
    text_t details = {0}, body = {0}, headers = {0}, credentials = {0};
    append_authorization_details(&details, oauth->table);
    append_str(&body, "grant_type=client_credentials&scope=all-apis&resource=");
    char resource[256];
    (void)snprintf(resource, sizeof(resource),
                   "api://databricks/workspaces/%s/zerobusDirectWriteApi",
                   oauth->workspace_id);
    append_form(&body, resource);
    append_str(&body, "&authorization_details=");
    append_form(&body, details.failed ? "" : details.data);
    append_str(&credentials, oauth->client_id);
    append_str(&credentials, ":");
    append_str(&credentials, oauth->client_secret);
    append_str(&headers, "Authorization: Basic ");
    if (!credentials.failed) {
        append_base64(&headers, credentials.data, credentials.len);
    }
    append_str(&headers, "\r\nAccept: application/json\r\n");
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    zb_http_response_t response = {0};
    if (details.failed || body.failed || headers.failed || credentials.failed) {
        status = ZEROBUS_STATUS_OUT_OF_MEMORY;
    } else {
        status = zb_http_post(
            oauth->tls ? oauth->tls_context : NULL, oauth->host, oauth->port,
            TOKEN_PATH, headers.data, "application/x-www-form-urlencoded",
            (const uint8_t *)body.data, body.len, deadline, &response);
    }
    free(details.data);
    zb_secure_free(body.data, body.capacity);
    zb_secure_free(headers.data, headers.capacity);
    zb_secure_free(credentials.data, credentials.capacity);
    if (status != ZEROBUS_STATUS_OK) {
        return zb_fail(out_error, status,
                       "OAuth token request to %s failed (status %u)",
                       oauth->host, (unsigned)status);
    }
    if (response.status != 200) {
        status = http_status_code(response.status);
        int shown = response.body_len > 512 ? 512 : (int)response.body_len;
        zb_fail(out_error, status, "OAuth token request returned HTTP %u: %.*s",
                (unsigned)response.status, shown, response.body);
        zb_http_response_clear(&response);
        return status;
    }
    char *token = NULL;
    uint64_t expires_in = 0;
    if (!zb_json_get_string(response.body, response.body_len, "access_token",
                            &token) ||
        token[0] == '\0') {
        zb_secure_free_cstr(token);
        zb_http_response_clear(&response);
        return zb_fail(out_error, ZEROBUS_STATUS_UNAUTHENTICATED,
                       "OAuth token response has no access_token");
    }
    for (const char *p = token; *p; ++p) {
        if ((unsigned char)*p < 0x21 || (unsigned char)*p > 0x7e) {
            zb_secure_free_cstr(token);
            zb_http_response_clear(&response);
            return zb_fail(out_error, ZEROBUS_STATUS_UNAUTHENTICATED,
                           "OAuth access token is not a valid header value");
        }
    }
    if (!zb_json_get_u64(response.body, response.body_len, "expires_in",
                         &expires_in)) {
        expires_in = 0;
    }
    zb_http_response_clear(&response);
    zb_secure_free_cstr(oauth->token);
    oauth->token = token;
    *out_expires_in = expires_in;
    return ZEROBUS_STATUS_OK;
}

/* ---- lifecycle ---------------------------------------------------------- */

zerobus_status_t zb_oauth_new(const char *uc_endpoint, const char *workspace_id,
                              zerobus_string_view_t table,
                              zerobus_string_view_t client_id,
                              zerobus_string_view_t client_secret,
                              zb_oauth_t **out_oauth)
{
    zb_oauth_t *oauth = calloc(1, sizeof(*oauth));
    if (!oauth) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status = zb_mutex_init(&oauth->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        free(oauth);
        return status;
    }
    oauth->workspace_id = zb_strdup(workspace_id);
    oauth->table = zb_strdup_view(table);
    oauth->client_id = zb_strdup_view(client_id);
    oauth->client_secret = zb_strdup_view(client_secret);
    if (!zb_url_split(uc_endpoint, &oauth->tls, &oauth->host, &oauth->port) ||
        !oauth->workspace_id || !oauth->table || !oauth->client_id ||
        !oauth->client_secret) {
        zb_oauth_free(oauth);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    *out_oauth = oauth;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_oauth_token(zb_oauth_t *oauth, zb_deadline_t deadline,
                                char **out_token, zerobus_error_t **out_error)
{
    zerobus_status_t status = zb_mutex_lock(&oauth->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (!oauth->token || zb_deadline_expired(oauth->refresh_at)) {
        zb_deadline_t limit = zb_deadline_after_ms(REQUEST_TIMEOUT_MS);
        uint64_t expires_in = 0;
        status = mint(oauth, deadline < limit ? deadline : limit, &expires_in,
                      out_error);
        if (status == ZEROBUS_STATUS_OK) {
            /* Refresh well before expiry; without a lifetime, mint every
             * time. */
            uint64_t margin = expires_in / 4 < 300 ? expires_in / 4 : 300;
            oauth->refresh_at =
                expires_in ? zb_deadline_after_seconds(expires_in - margin)
                           : ZB_DEADLINE_IMMEDIATE;
            ZB_INFO("minted OAuth token for %s, valid for %llus", oauth->table,
                    (unsigned long long)expires_in);
        }
    }
    if (status == ZEROBUS_STATUS_OK) {
        *out_token = zb_strdup(oauth->token);
        if (!*out_token) {
            status = ZEROBUS_STATUS_OUT_OF_MEMORY;
        }
    }
    (void)zb_mutex_unlock(&oauth->mutex);
    return status;
}

void zb_oauth_invalidate(zb_oauth_t *oauth)
{
    if (zb_mutex_lock(&oauth->mutex) == ZEROBUS_STATUS_OK) {
        zb_secure_free_cstr(oauth->token);
        oauth->token = NULL;
        (void)zb_mutex_unlock(&oauth->mutex);
    }
}

void zb_oauth_free(zb_oauth_t *oauth)
{
    if (!oauth) {
        return;
    }
    zb_tls_ctx_free(oauth->tls_context);
    free(oauth->host);
    free(oauth->workspace_id);
    free(oauth->table);
    free(oauth->client_id);
    zb_secure_free_cstr(oauth->client_secret);
    zb_secure_free_cstr(oauth->token);
    (void)zb_mutex_destroy(&oauth->mutex);
    free(oauth);
}
