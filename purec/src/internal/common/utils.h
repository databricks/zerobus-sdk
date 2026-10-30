/*
 * Input validation, text, and owned-memory helpers shared across the SDK.
 * Local checks only: no transport, OAuth, protobuf, TLS or threading code
 * lives here.
 */
#ifndef ZB_UTILS_H
#define ZB_UTILS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "zerobus/common.h"

/* ZB_STATIC marks a helper `static` in the shipped build, and external under a
 * ZB_TESTING build so unit tests can call it directly (its prototype then goes
 * in the ZB_TESTING block at the end of this header).
 *
 * ZB_TESTING only changes linkage and adds declarations, never guards behavior,
 * so the testing build stays behaviorally identical to what ships. A real test
 * seam (fault injection, a mock clock) uses a function pointer or link
 * substitution, not #ifdef ZB_TESTING. */
#ifdef ZB_TESTING
#define ZB_STATIC
#else
#define ZB_STATIC static
#endif

/* {NULL, 0} is empty; {NULL, nonzero} is invalid and also reported as empty. */
bool zb_is_empty(zerobus_string_view_t view);

/* Non-empty, well-formed UTF-8, no embedded NUL. */
bool zb_is_valid_string(zerobus_string_view_t view);

/* True if s (len bytes) begins with prefix, matched case-insensitively. prefix
 * is NUL-terminated and must be lowercase (only s is folded). */
bool zb_starts_with(const char *s, size_t len, const char *prefix);

/* True if s (len bytes) equals lower, matched case-insensitively. lower is
 * NUL-terminated and must be lowercase (only s is folded). */
bool zb_equals_ignore_case(const char *s, size_t len, const char *lower);

/* True if value is exactly literal, compared case-sensitively. {NULL, 0}
 * equals ""; {NULL, nonzero} equals nothing. */
bool zb_view_equals(zerobus_string_view_t value, const char *literal);

/* True if every byte is printable ASCII (0x20 to 0x7E), as gRPC requires for
 * header values. {NULL, 0} is allowed; {NULL, nonzero} is not. */
bool zb_is_header_value(zerobus_string_view_t view);

/* Parses all len bytes as an unsigned decimal into *out. False, leaving *out
 * unchanged, if len is 0, a byte is not a digit, or the value exceeds
 * UINT64_MAX. */
bool zb_parse_u64(const uint8_t *value, size_t len, uint64_t *out);

/* Like zb_parse_u64, but also false for a value over UINT32_MAX. */
bool zb_parse_u32(const uint8_t *value, size_t len, uint32_t *out);

/* The value of hex digit c, in either case, or -1 if c is not one. */
int zb_hex_value(char c);

/* The view without leading or trailing ASCII whitespace. A view with NULL data
 * is returned unchanged. */
zerobus_string_view_t zb_trim_whitespace(zerobus_string_view_t view);

/*
 * Report whether a table name is a fully qualified catalog.schema.table: three
 * non-empty, dot-separated components. This is a simplified structural check —
 * it does not parse backtick-quoted identifiers or validate character sets.
 */
bool zb_table_name_is_valid(zerobus_string_view_t table);

/* Outcome of validating an endpoint URL. */
typedef enum {
    ZB_URL_OK = 0,
    ZB_URL_NO_SCHEME,  /* neither "http://" nor "https://" */
    ZB_URL_EMPTY_HOST, /* nothing between the scheme and the port */
    ZB_URL_BAD_HOST,   /* empty label, userinfo, IPv6 literal, or a bad char */
    ZB_URL_BAD_PORT,   /* ":port" is empty or not a number in 1..65535 */
    ZB_URL_HAS_PATH    /* a path, query, fragment, or trailing '/' after host */
} zb_url_result;

/*
 * Validate that endpoint is a bare "http(s)://host[:port]" origin. Anything
 * after the host and optional port — a path, query, fragment, or even a
 * trailing "/" — is rejected (ZB_URL_HAS_PATH), so the accepted string is safe
 * to store and append a path to (e.g. the OAuth "/oidc/v1/token").
 */
zb_url_result zb_url_validate(zerobus_string_view_t endpoint);

/* Copies as much of data[*consumed, len) as fits into buffer[*used, size),
 * and advances *used and *consumed by the number of bytes copied. */
void zb_copy_bounded(uint8_t *buffer, size_t size, size_t *used,
                     const uint8_t *data, size_t len, size_t *consumed);

/* Replace *target with a copy of view, freeing the previous value. False on
 * OOM, leaving *target untouched. */
bool zb_replace_string(char **target, zerobus_string_view_t view);

/* Duplicate len bytes of src into a fresh NUL-terminated buffer. A NULL src
 * yields NULL (any len); a non-NULL src with len 0 yields ""; NULL on OOM. */
char *zb_strndup(const char *src, size_t len);

/* zb_strndup(view.data, view.len). */
char *zb_strdup_view(zerobus_string_view_t view);

/* zb_strndup(s, strlen(s)), except a NULL s yields NULL. */
char *zb_strdup(const char *s);

/* Overwrite len bytes at p in a way the optimizer may not elide. Used for OAuth
 * secrets before freeing. NULL-safe. */
void zb_secure_zero(void *p, size_t len);

/* Free after securely zeroing len bytes. NULL-safe. */
void zb_secure_free(void *p, size_t len);

/* zb_secure_free for a NUL-terminated string: zeroes strlen(s) bytes.
 * NULL-safe. */
void zb_secure_free_cstr(char *s);

#ifdef ZB_TESTING
bool is_valid_utf8(const char *text, size_t len);
bool host_labels_are_valid(const char *host, size_t len);
#endif

#endif /* ZB_UTILS_H */
