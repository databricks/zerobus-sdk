/* Unit tests for the input-validation and owned-string helpers in utils.c. */
#include <stdlib.h>
#include <string.h>

#include "test_common.h"
#include "utils.h"

/* ---- helpers ----------------------------------------------------------- */

static bool utf8_ok(const char *s)
{
    return is_valid_utf8(s, strlen(s));
}

static bool labels_ok(const char *s)
{
    return host_labels_are_valid(s, strlen(s));
}

/* ---- tests ------------------------------------------------------------- */

static void test_view_classification(void)
{
    CHECK(zb_is_empty((zerobus_string_view_t){NULL, 0}));
    CHECK(zb_is_empty((zerobus_string_view_t){"x", 0}));
    CHECK(zb_is_empty((zerobus_string_view_t){NULL, 5})); /* invalid */
    CHECK(!zb_is_empty(sv("x")));

    CHECK(zb_is_valid_string(sv("hello")));
    CHECK(!zb_is_valid_string((zerobus_string_view_t){NULL, 0}));
    /* An embedded NUL makes an otherwise non-empty view invalid. */
    CHECK(!zb_is_valid_string((zerobus_string_view_t){"a\0b", 3}));
}

static void test_utf8(void)
{
    CHECK(is_valid_utf8(NULL, 0));  /* NULL with zero length is vacuously ok */
    CHECK(!is_valid_utf8(NULL, 1)); /* NULL with a length is not */
    CHECK(utf8_ok(""));             /* empty run of bytes is valid UTF-8 */
    CHECK(utf8_ok("plain ascii"));
    CHECK(utf8_ok("caf\xC3\xA9"));         /* café (2-byte) */
    CHECK(utf8_ok("\xE2\x9C\x93"));        /* check mark (3-byte) */
    CHECK(utf8_ok("\xF0\x9F\x98\x80"));    /* emoji (4-byte) */
    CHECK(utf8_ok("caf\xC3\xA9 au lait")); /* multi-byte then more text */
    CHECK(utf8_ok("\xF4\x8F\xBF\xBF"));    /* U+10FFFF, the maximum */

    CHECK(!is_valid_utf8("a\0b", 3));             /* embedded NUL */
    CHECK(!is_valid_utf8("\x80", 1));             /* stray continuation byte */
    CHECK(!is_valid_utf8("\xFF", 1));             /* invalid lead byte */
    CHECK(!is_valid_utf8("\xC3", 1));             /* truncated 2-byte */
    CHECK(!is_valid_utf8("\xC0\xAF", 2));         /* overlong 2-byte */
    CHECK(!is_valid_utf8("\xE0\x80\x80", 3));     /* overlong 3-byte (E0) */
    CHECK(!is_valid_utf8("\xF0\x80\x80\x80", 4)); /* overlong 4-byte (F0) */
    CHECK(!is_valid_utf8("\xED\xA0\x80", 3));     /* UTF-16 surrogate U+D800 */
    CHECK(!is_valid_utf8("\xF5\x80\x80\x80", 4)); /* lead > F4 */
    CHECK(!is_valid_utf8("\xF4\x90\x80\x80", 4)); /* > U+10FFFF (F4 bound) */
    CHECK(!is_valid_utf8("\xE2\x28\xA1", 3));     /* bad first continuation */
    CHECK(!is_valid_utf8("\xE2\x9C\x28", 3));     /* bad later continuation */
}

static void test_table_name(void)
{
    CHECK(zb_table_name_is_valid(sv("catalog.schema.table")));
    CHECK(zb_table_name_is_valid(sv("c.s.t")));

    CHECK(!zb_table_name_is_valid(sv("schema.table")));    /* 2 parts */
    CHECK(!zb_table_name_is_valid(sv("a.b.c.d")));         /* 4 parts */
    CHECK(!zb_table_name_is_valid(sv("catalog..table")));  /* empty middle */
    CHECK(!zb_table_name_is_valid(sv(".schema.table")));   /* leading dot */
    CHECK(!zb_table_name_is_valid(sv("catalog.schema."))); /* trailing dot */
    CHECK(!zb_table_name_is_valid(sv("catalog")));         /* no dots */
    CHECK(!zb_table_name_is_valid((zerobus_string_view_t){NULL, 0}));
}

static void test_url_validate(void)
{
    /* Accepted: bare origins, http or https, with or without a port, and a
     * single-label host (no workspace-subdomain requirement). */
    CHECK_EQ_INT(
        zb_url_validate(sv("https://ws.zerobus.r.cloud.databricks.com")),
        ZB_URL_OK);
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com:8443")),
                 ZB_URL_OK);
    CHECK_EQ_INT(zb_url_validate(sv("http://host.example.com")), ZB_URL_OK);
    CHECK_EQ_INT(zb_url_validate(sv("http://localhost:8080")), ZB_URL_OK);
    CHECK_EQ_INT(zb_url_validate(sv("https://localhost")), ZB_URL_OK);
    /* A single trailing dot is the DNS root form. */
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com.")), ZB_URL_OK);
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com:443")),
                 ZB_URL_OK);
    /* The scheme is case-insensitive (RFC 3986 §3.1). */
    CHECK_EQ_INT(zb_url_validate(sv("HTTPS://host.example.com")), ZB_URL_OK);
    CHECK_EQ_INT(zb_url_validate(sv("HtTp://localhost")), ZB_URL_OK);

    /* No scheme, an unsupported one, a bare truncated scheme, or no input. */
    CHECK_EQ_INT(zb_url_validate(sv("host.example.com")), ZB_URL_NO_SCHEME);
    CHECK_EQ_INT(zb_url_validate(sv("ftp://host.example.com")),
                 ZB_URL_NO_SCHEME);
    CHECK_EQ_INT(zb_url_validate(sv("http")), ZB_URL_NO_SCHEME);
    CHECK_EQ_INT(zb_url_validate((zerobus_string_view_t){NULL, 0}),
                 ZB_URL_NO_SCHEME);

    /* Missing host. */
    CHECK_EQ_INT(zb_url_validate(sv("https://")), ZB_URL_EMPTY_HOST);
    CHECK_EQ_INT(zb_url_validate(sv("https:///path")), ZB_URL_EMPTY_HOST);
    CHECK_EQ_INT(zb_url_validate(sv("https://:443")), ZB_URL_EMPTY_HOST);

    /* Empty labels are not a valid hostname; a malformed host is reported even
     * when a path also follows. */
    CHECK_EQ_INT(zb_url_validate(sv("https://a..b")), ZB_URL_BAD_HOST);
    CHECK_EQ_INT(zb_url_validate(sv("https://.a")), ZB_URL_BAD_HOST);
    CHECK_EQ_INT(zb_url_validate(sv("https://a..b:443/p")), ZB_URL_BAD_HOST);

    /* Userinfo, IPv6 literals, and forbidden host characters are rejected. */
    CHECK_EQ_INT(zb_url_validate(sv("https://user@host.example.com")),
                 ZB_URL_BAD_HOST);
    /* Userinfo with a password: '@' is caught before the ':' split, so this is
     * BAD_HOST, not a bogus BAD_PORT. */
    CHECK_EQ_INT(zb_url_validate(sv("https://user:pass@host.example.com")),
                 ZB_URL_BAD_HOST);
    CHECK_EQ_INT(zb_url_validate(sv("https://[::1]")), ZB_URL_BAD_HOST);
    CHECK_EQ_INT(zb_url_validate(sv("https://ho st.example.com")),
                 ZB_URL_BAD_HOST);
    CHECK_EQ_INT(zb_url_validate(sv("https://ho^st.example.com")),
                 ZB_URL_BAD_HOST);

    /* A ":port" must be a number in 1..65535. */
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com:")),
                 ZB_URL_BAD_PORT);
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com:abc")),
                 ZB_URL_BAD_PORT);
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com:99999")),
                 ZB_URL_BAD_PORT);
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com:0")),
                 ZB_URL_BAD_PORT);

    /* Only a bare origin is accepted: a path, query, fragment, or even a
     * trailing '/' is rejected. */
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com/")),
                 ZB_URL_HAS_PATH);
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com/path")),
                 ZB_URL_HAS_PATH);
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com:8443/path")),
                 ZB_URL_HAS_PATH);
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com?x=1")),
                 ZB_URL_HAS_PATH);
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com#frag")),
                 ZB_URL_HAS_PATH);
    /* Backslash is a path delimiter for http/https (WHATWG '\' == '/'), so it
     * ends the authority like '/'. */
    CHECK_EQ_INT(zb_url_validate(sv("https://host.example.com\\path")),
                 ZB_URL_HAS_PATH);
}

static void test_host_labels(void)
{
    CHECK(labels_ok("a"));
    CHECK(labels_ok("host.example.com"));
    CHECK(labels_ok("host.example.com.")); /* trailing dot: DNS root form */

    CHECK(!labels_ok(""));     /* empty */
    CHECK(!labels_ok("."));    /* only a dot */
    CHECK(!labels_ok(".a"));   /* leading empty label */
    CHECK(!labels_ok("a..b")); /* interior empty label */
}

static void test_strdup(void)
{
    /* Each variant duplicates a non-NULL input into a fresh copy. */
    char *hello = zb_strndup("hello", 5);
    CHECK(hello != NULL && strcmp(hello, "hello") == 0);
    free(hello);

    /* Partial copy is NUL-terminated. */
    char *hel = zb_strndup("hello", 3);
    CHECK(hel != NULL && strcmp(hel, "hel") == 0);
    free(hel);

    char *from_str = zb_strdup("hello");
    CHECK(from_str != NULL && strcmp(from_str, "hello") == 0);
    free(from_str);

    char *from_view = zb_strdup_view(sv("hello"));
    CHECK(from_view != NULL && strcmp(from_view, "hello") == 0);
    free(from_view);

    /* A non-NULL pointer with length 0 yields "". */
    char *empty = zb_strndup("", 0);
    CHECK(empty != NULL && empty[0] == '\0');
    free(empty);

    char *empty_view = zb_strdup_view(sv(""));
    CHECK(empty_view != NULL && empty_view[0] == '\0');
    free(empty_view);

    /* A NULL pointer yields NULL, across every variant and length. */
    CHECK(zb_strndup(NULL, 0) == NULL);
    CHECK(zb_strndup(NULL, 5) == NULL);
    CHECK(zb_strdup_view((zerobus_string_view_t){NULL, 0}) == NULL);
    CHECK(zb_strdup_view((zerobus_string_view_t){NULL, 5}) == NULL);
    CHECK(zb_strdup(NULL) == NULL);
}

static void test_secure(void)
{
    /* Early-out paths must be safe. */
    zb_secure_zero(NULL, 8);
    zb_secure_free(NULL, 0);
    zb_secure_free_cstr(NULL); /* NULL-safe */

    char *p = zb_strndup("secret", 6);
    CHECK(p != NULL);
    if (p != NULL) {
        zb_secure_zero(p, 6); /* the actual overwrite loop */
        CHECK(p[0] == '\0');
        zb_secure_free(p, 6);
    }

    /* zb_secure_free_cstr zeroes strlen(s) bytes and frees a real copy. */
    zb_secure_free_cstr(zb_strndup("tmp", 3));
    CHECK(1);
}

int main(void)
{
    test_view_classification();
    test_utf8();
    test_table_name();
    test_url_validate();
    test_host_labels();
    test_strdup();
    test_secure();
    TEST_MAIN_RETURN();
}
