#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

/* ---- UTF-8 -------------------------------------------------------------- */

ZB_STATIC bool is_valid_utf8(const char *text, size_t len)
{
    /* Byte inspection below needs unsigned arithmetic: char is signed on some
     * platforms, which would break the lead/continuation-byte masks. */
    const uint8_t *bytes = (const uint8_t *)text;
    if (bytes == NULL) {
        return len == 0;
    }
    for (size_t i = 0; i < len; i++) {
        uint8_t c = bytes[i];
        if (c == 0x00) {
            return false; /* embedded NUL */
        }
        if (c < 0x80) {
            continue;
        }
        size_t extra;
        uint8_t lo, hi;
        if ((c & 0xE0) == 0xC0) {
            extra = 1;
            lo = 0x80;
            hi = 0xBF;
            if (c < 0xC2) {
                return false; /* overlong 2-byte */
            }
        } else if ((c & 0xF0) == 0xE0) {
            extra = 2;
            /* Constrain the first continuation byte to reject overlong forms
             * and UTF-16 surrogates. */
            lo = (c == 0xE0) ? 0xA0 : 0x80;
            hi = (c == 0xED) ? 0x9F : 0xBF;
        } else if ((c & 0xF8) == 0xF0) {
            extra = 3;
            lo = (c == 0xF0) ? 0x90 : 0x80;
            hi = (c == 0xF4) ? 0x8F : 0xBF;
            if (c > 0xF4) {
                return false; /* > U+10FFFF */
            }
        } else {
            return false; /* invalid lead byte or stray continuation */
        }
        if (extra >= len - i) {
            return false; /* truncated sequence */
        }
        uint8_t first = bytes[i + 1];
        if (first < lo || first > hi) {
            return false;
        }
        for (size_t k = 2; k <= extra; ++k) {
            uint8_t cc = bytes[i + k];
            if ((cc & 0xC0) != 0x80) {
                return false;
            }
        }
        i += extra; /* the loop's i++ accounts for the lead byte */
    }
    return true;
}

/* ---- view classification ----------------------------------------------- */

bool zb_is_empty(zerobus_string_view_t view)
{
    return view.data == NULL || view.len == 0;
}

bool zb_is_valid_string(zerobus_string_view_t view)
{
    if (zb_is_empty(view)) {
        return false;
    }
    return is_valid_utf8(view.data, view.len);
}

/* ---- table name -------------------------------------------------------- */

bool zb_table_name_is_valid(zerobus_string_view_t table)
{
    if (!zb_is_valid_string(table)) {
        return false;
    }
    /* Exactly three non-empty, dot-separated components. */
    size_t component_len = 0;
    size_t components = 0;
    for (size_t i = 0; i < table.len; ++i) {
        if (table.data[i] == '.') {
            if (component_len == 0) {
                return false; /* empty component (leading, trailing, or "..") */
            }
            components++;
            component_len = 0;
        } else {
            component_len++;
        }
    }
    if (component_len == 0) {
        return false; /* trailing dot */
    }
    components++;
    return components == 3;
}

/* ---- endpoint URL ------------------------------------------------------- */

/* Non-empty dot-separated labels, with one optional trailing dot (the DNS root
 * form). Rejects "a..b", ".a" and "". */
ZB_STATIC bool host_labels_are_valid(const char *host, size_t len)
{
    size_t label = 0;
    for (size_t i = 0; i < len; ++i) {
        if (host[i] == '.') {
            if (label == 0) {
                return false;
            }
            label = 0;
        } else {
            label++;
        }
    }
    /* Empty labels (leading or interior) are already rejected in the loop, so
     * reaching here with a non-empty host means every label is valid. */
    return len > 0;
}

/* True if s (len bytes) begins with prefix, matched case-insensitively. prefix
 * is NUL-terminated and must be lowercase (only s is folded). */
static bool starts_with(const char *s, size_t len, const char *prefix)
{
    for (size_t i = 0; prefix[i] != '\0'; ++i) {
        if (i >= len) {
            return false;
        }
        char c = s[i];
        if (c >= 'A' && c <= 'Z') {
            c = (char)(c - 'A' + 'a');
        }
        if (c != prefix[i]) {
            return false;
        }
    }
    return true;
}

zb_url_result zb_url_validate(zerobus_string_view_t endpoint)
{
    if (endpoint.data == NULL) {
        return ZB_URL_NO_SCHEME;
    }

    const char *rest;
    size_t rest_len;
    if (starts_with(endpoint.data, endpoint.len, "https://")) {
        rest = endpoint.data + 8;
        rest_len = endpoint.len - 8;
    } else if (starts_with(endpoint.data, endpoint.len, "http://")) {
        rest = endpoint.data + 7;
        rest_len = endpoint.len - 7;
    } else {
        return ZB_URL_NO_SCHEME;
    }

    /* The authority ends at the first '/', '\', '?' or '#' (or the end). For
     * http/https, WHATWG treats '\' like '/', so it delimits the authority too.
     */
    size_t authority_len = 0;
    while (authority_len < rest_len && rest[authority_len] != '/' &&
           rest[authority_len] != '\\' && rest[authority_len] != '?' &&
           rest[authority_len] != '#') {
        ++authority_len;
    }
    if (authority_len == 0) {
        return ZB_URL_EMPTY_HOST;
    }

    /* Userinfo ("user[:pass]@host") is not supported. Reject '@' across the
     * whole authority, before the ':' split, so "user:pass@host" is BAD_HOST
     * and not a bogus BAD_PORT (the first ':' is inside the userinfo). */
    if (memchr(rest, '@', authority_len) != NULL) {
        return ZB_URL_BAD_HOST;
    }

    /* The host is the authority up to an optional ":port". */
    const char *colon = (const char *)memchr(rest, ':', authority_len);
    size_t host_len = colon != NULL ? (size_t)(colon - rest) : authority_len;
    if (host_len == 0) {
        return ZB_URL_EMPTY_HOST;
    }
    /* Reject control/space and the WHATWG forbidden host code points reachable
     * here: [ ] < > ^ | ('/', '\', '?', '#' already ended the authority, ':'
     * split off the port, '@' was rejected above). Forbidding brackets also
     * rejects IPv6 literals. */
    for (size_t i = 0; i < host_len; ++i) {
        unsigned char c = (unsigned char)rest[i];
        if (c <= ' ' || c == 0x7f || c == '[' || c == ']' || c == '<' ||
            c == '>' || c == '^' || c == '|') {
            return ZB_URL_BAD_HOST;
        }
    }
    if (!host_labels_are_valid(rest, host_len)) {
        return ZB_URL_BAD_HOST;
    }

    /* A present port must be a non-empty run of digits in 1..65535. */
    if (host_len < authority_len) {
        if (host_len + 1 == authority_len) { /* ':' with nothing after it */
            return ZB_URL_BAD_PORT;
        }
        unsigned long port = 0;
        for (size_t i = host_len + 1; i < authority_len; ++i) {
            if (rest[i] < '0' || rest[i] > '9') {
                return ZB_URL_BAD_PORT;
            }
            port = port * 10u + (unsigned long)(rest[i] - '0');
            if (port > 65535u) {
                return ZB_URL_BAD_PORT;
            }
        }
        if (port == 0) {
            return ZB_URL_BAD_PORT;
        }
    }

    /* Only a bare origin is allowed: reject a path, query, fragment, or even a
     * trailing '/' so the accepted string is safe to append a path to. */
    if (authority_len != rest_len) {
        return ZB_URL_HAS_PATH;
    }
    return ZB_URL_OK;
}

/* ---- owned-memory helpers ---------------------------------------------- */

bool zb_replace_string(char **target, zerobus_string_view_t view)
{
    char *copy = zb_strdup_view(view);
    if (copy == NULL) {
        return false;
    }
    free(*target);
    *target = copy;
    return true;
}

char *zb_strndup(const char *src, size_t len)
{
    if (src == NULL || len == SIZE_MAX) {
        return NULL; /* len + 1 would wrap to 0 */
    }
    char *out = (char *)malloc(len + 1);
    if (out == NULL) {
        return NULL;
    }
    memcpy(out, src, len);
    out[len] = '\0';
    return out;
}

char *zb_strdup_view(zerobus_string_view_t view)
{
    return zb_strndup(view.data, view.len);
}

char *zb_strdup(const char *s)
{
    if (s == NULL) {
        return NULL;
    }
    return zb_strndup(s, strlen(s));
}

/*
 * A volatile pointer stops a compiler from treating the store as dead. It is
 * the plain-C stand-in for the platform explicit_bzero / SecureZeroMemory the
 * production layer would use.
 */
void zb_secure_zero(void *p, size_t len)
{
    if (p == NULL || len == 0) {
        return;
    }
    volatile unsigned char *q = (volatile unsigned char *)p;
    while (len-- > 0) {
        *q++ = 0;
    }
}

void zb_secure_free(void *p, size_t len)
{
    if (p == NULL) {
        return;
    }
    zb_secure_zero(p, len);
    free(p);
}

void zb_secure_free_cstr(char *s)
{
    zb_secure_free(s, s != NULL ? strlen(s) : 0);
}
