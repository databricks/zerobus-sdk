#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

/* ---- UTF-8 -------------------------------------------------------------- */

/*
 * A table-driven UTF-8 automaton. Each state is a bit offset into a 64-bit
 * row; the row for a byte holds, at each state's offset, the 6-bit state that
 * byte leads to, so one shift and mask take a step. Zero, ERROR, is the
 * default: a row lists only the transitions that stay valid. ACCEPT expects
 * a lead byte; CONT1..CONT3 expect that many continuation bytes; the
 * remaining states constrain the first continuation of E0, ED, F0 and F4 to
 * reject overlong forms, UTF-16 surrogates and code points past U+10FFFF.
 */
enum {
    ERROR = 0,
    ACCEPT = 6,
    CONT1 = 12,
    CONT2 = 18,
    CONT3 = 24,
    AFTER_E0 = 30,
    AFTER_ED = 36,
    AFTER_F0 = 42,
    AFTER_F4 = 48
};

#define STEP(from, to) ((uint64_t)(to) << (from))

/* Rows by byte class; see byte_class. */
static const uint64_t utf8_rows[12] = {
    /* 0: NUL, C0, C1, F5..FF */
    0,
    /* 1: ASCII 01..7F */
    STEP(ACCEPT, ACCEPT),
    /* 2: continuation 80..8F */
    STEP(CONT1, ACCEPT) | STEP(CONT2, CONT1) | STEP(CONT3, CONT2) |
        STEP(AFTER_ED, CONT1) | STEP(AFTER_F4, CONT2),
    /* 3: continuation 90..9F */
    STEP(CONT1, ACCEPT) | STEP(CONT2, CONT1) | STEP(CONT3, CONT2) |
        STEP(AFTER_ED, CONT1) | STEP(AFTER_F0, CONT2),
    /* 4: continuation A0..BF */
    STEP(CONT1, ACCEPT) | STEP(CONT2, CONT1) | STEP(CONT3, CONT2) |
        STEP(AFTER_E0, CONT1) | STEP(AFTER_F0, CONT2),
    /* 5: C2..DF */
    STEP(ACCEPT, CONT1),
    /* 6: E0 */
    STEP(ACCEPT, AFTER_E0),
    /* 7: E1..EC, EE, EF */
    STEP(ACCEPT, CONT2),
    /* 8: ED */
    STEP(ACCEPT, AFTER_ED),
    /* 9: F0 */
    STEP(ACCEPT, AFTER_F0),
    /* 10: F1..F3 */
    STEP(ACCEPT, CONT3),
    /* 11: F4 */
    STEP(ACCEPT, AFTER_F4),
};

/* The row index of each byte value. */
static const uint8_t byte_class[256] = {
    0, 1,  1,  1,  1,  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 00 */
    1, 1,  1,  1,  1,  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 10 */
    1, 1,  1,  1,  1,  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 20 */
    1, 1,  1,  1,  1,  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 30 */
    1, 1,  1,  1,  1,  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 40 */
    1, 1,  1,  1,  1,  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 50 */
    1, 1,  1,  1,  1,  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 60 */
    1, 1,  1,  1,  1,  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 70 */
    2, 2,  2,  2,  2,  2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, /* 80 */
    3, 3,  3,  3,  3,  3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, /* 90 */
    4, 4,  4,  4,  4,  4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, /* A0 */
    4, 4,  4,  4,  4,  4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, /* B0 */
    0, 0,  5,  5,  5,  5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, /* C0 */
    5, 5,  5,  5,  5,  5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, /* D0 */
    6, 7,  7,  7,  7,  7, 7, 7, 7, 7, 7, 7, 7, 8, 7, 7, /* E0 */
    9, 10, 10, 10, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* F0 */
};

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#include <immintrin.h>
#define ZB_UTF8_AVX2 1

/*
 * With AVX2, 32 bytes at a time by the lookup algorithm of Keiser and Lemire,
 * "Validating UTF-8 In Less Than One Instruction Per Byte" (2021), as in
 * simdjson. Each bit flags one kind of error a byte and its predecessor can
 * reveal; three lookups by nibble, of the predecessor's high and low nibble
 * and of the byte's high nibble, each keep the kinds their nibble allows, so
 * a bit survives all three only where that error occurs. Missing and excess
 * continuations show against the bytes two and three back.
 */
#define TOO_SHORT 0x01      /* 11______ 0_______, 11______ 11______ */
#define TOO_LONG 0x02       /* 0_______ 10______ */
#define OVERLONG_3 0x04     /* 11100000 100_____ */
#define TOO_LARGE 0x08      /* 11110100 1001____ and beyond U+10FFFF */
#define SURROGATE 0x10      /* 11101101 101_____ */
#define OVERLONG_2 0x20     /* 1100000_ 10______ */
#define TOO_LARGE_1000 0x40 /* 11110101 1000____ and up */
#define OVERLONG_4 0x40     /* 11110000 1000____ */
#define TWO_CONTS 0x80      /* 10______ 10______ */
#define CARRY (TOO_SHORT | TOO_LONG | TWO_CONTS)
#define LARGE (CARRY | TOO_LARGE | TOO_LARGE_1000)

#define LOOKUP(...) _mm256_setr_epi8(__VA_ARGS__, __VA_ARGS__)

/* The 32 bytes ending n bytes before input, previous holding those before. */
#define PREVIOUS(input, previous, n)                                           \
    _mm256_alignr_epi8((input),                                                \
                       _mm256_permute2x128_si256((previous), (input), 0x21),   \
                       16 - (n))

__attribute__((target("avx2"))) static __m256i high_nibbles(__m256i bytes)
{
    return _mm256_and_si256(_mm256_srli_epi16(bytes, 4), _mm256_set1_epi8(15));
}

/* Nonzero where input is invalid after previous, the 32 bytes before it. */
__attribute__((target("avx2"))) static __m256i utf8_errors(__m256i input,
                                                           __m256i previous)
{
    const __m256i byte_1_high = LOOKUP(
        TOO_LONG, TOO_LONG, TOO_LONG, TOO_LONG, TOO_LONG, TOO_LONG, TOO_LONG,
        TOO_LONG, TWO_CONTS, TWO_CONTS, TWO_CONTS, TWO_CONTS,
        TOO_SHORT | OVERLONG_2, TOO_SHORT, TOO_SHORT | OVERLONG_3 | SURROGATE,
        TOO_SHORT | TOO_LARGE | TOO_LARGE_1000 | OVERLONG_4);
    const __m256i byte_1_low =
        LOOKUP(CARRY | OVERLONG_3 | OVERLONG_2 | OVERLONG_4, CARRY | OVERLONG_2,
               CARRY, CARRY, CARRY | TOO_LARGE, LARGE, LARGE, LARGE, LARGE,
               LARGE, LARGE, LARGE, LARGE, LARGE | SURROGATE, LARGE, LARGE);
    const __m256i byte_2_high =
        LOOKUP(TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT,
               TOO_SHORT, TOO_SHORT,
               TOO_LONG | OVERLONG_2 | TWO_CONTS | OVERLONG_3 | TOO_LARGE_1000 |
                   OVERLONG_4,
               TOO_LONG | OVERLONG_2 | TWO_CONTS | OVERLONG_3 | TOO_LARGE,
               TOO_LONG | OVERLONG_2 | TWO_CONTS | SURROGATE | TOO_LARGE,
               TOO_LONG | OVERLONG_2 | TWO_CONTS | SURROGATE | TOO_LARGE,
               TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT);
    __m256i prev1 = PREVIOUS(input, previous, 1);
    __m256i special = _mm256_and_si256(
        _mm256_and_si256(
            _mm256_shuffle_epi8(byte_1_high, high_nibbles(prev1)),
            _mm256_shuffle_epi8(byte_1_low,
                                _mm256_and_si256(prev1, _mm256_set1_epi8(15)))),
        _mm256_shuffle_epi8(byte_2_high, high_nibbles(input)));
    /* Bit 7 where a lead two or three back requires a continuation here. */
    __m256i third = _mm256_subs_epu8(PREVIOUS(input, previous, 2),
                                     _mm256_set1_epi8(0xe0 - 0x80));
    __m256i fourth = _mm256_subs_epu8(PREVIOUS(input, previous, 3),
                                      _mm256_set1_epi8(0xf0 - 0x80));
    __m256i required = _mm256_and_si256(_mm256_or_si256(third, fourth),
                                        _mm256_set1_epi8((char)0x80));
    return _mm256_xor_si256(required, special);
}

/* Nonzero where the last three bytes of input start a character that needs
 * more bytes than the block has left. */
__attribute__((target("avx2"))) static __m256i utf8_incomplete(__m256i input)
{
    const __m256i last = _mm256_setr_epi8(
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, (char)(0xf0 - 1),
        (char)(0xe0 - 1), (char)(0xc0 - 1));
    return _mm256_subs_epu8(input, last);
}

__attribute__((target("avx2"))) static bool
is_valid_utf8_avx2(const uint8_t *bytes, size_t len)
{
    const __m256i zero = _mm256_setzero_si256();
    __m256i error = zero;
    __m256i nul = zero;
    __m256i previous = zero;
    __m256i incomplete = zero;
    uint8_t tail[32];
    for (size_t i = 0;; i += 32) {
        __m256i input;
        bool last = len - i < 32;
        if (last) {
            /* Spaces end any character still open with an error. */
            memset(tail, ' ', sizeof(tail));
            memcpy(tail, bytes + i, len - i);
            input = _mm256_loadu_si256((const __m256i *)tail);
        } else {
            input = _mm256_loadu_si256((const __m256i *)(bytes + i));
        }
        nul = _mm256_or_si256(nul, _mm256_cmpeq_epi8(input, zero));
        if (_mm256_movemask_epi8(input) == 0) {
            error = _mm256_or_si256(error, incomplete);
            incomplete = zero;
        } else {
            error = _mm256_or_si256(error, utf8_errors(input, previous));
            incomplete = utf8_incomplete(input);
        }
        previous = input;
        if (last) {
            break;
        }
    }
    return _mm256_testz_si256(_mm256_or_si256(error, nul),
                              _mm256_or_si256(error, nul)) != 0;
}
#endif

ZB_STATIC bool is_valid_utf8(const char *text, size_t len)
{
    /* Byte inspection below needs unsigned arithmetic: char is signed on some
     * platforms. */
    const uint8_t *bytes = (const uint8_t *)text;
    if (bytes == NULL) {
        return len == 0;
    }
#ifdef ZB_UTF8_AVX2
    if (len >= 64 && __builtin_cpu_supports("avx2")) {
        return is_valid_utf8_avx2(bytes, len);
    }
#endif
    uint64_t state = ACCEPT;
    size_t i = 0;
    while (i < len) {
        /* Between characters, skip eight bytes at a time while none is NUL
         * or non-ASCII. A NUL may also flag its neighbor through the borrow;
         * the automaton then decides. */
        if (state == ACCEPT && len - i >= 8) {
            uint64_t word;
            memcpy(&word, bytes + i, sizeof(word));
            if ((((word - UINT64_C(0x0101010101010101)) | word) &
                 UINT64_C(0x8080808080808080)) == 0) {
                i += 8;
                continue;
            }
        }
        size_t stop = len - i > 16 ? i + 16 : len;
        for (; i < stop; ++i) {
            state = (utf8_rows[byte_class[bytes[i]]] >> state) & 63;
        }
        if (state == ERROR) {
            return false;
        }
    }
    return state == ACCEPT; /* Not inside a truncated sequence. */
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

bool zb_view_equals(zerobus_string_view_t value, const char *literal)
{
    size_t len = strlen(literal);
    return value.len == len &&
           (len == 0 ||
            (value.data != NULL && memcmp(value.data, literal, len) == 0));
}

/* ---- parsing ----------------------------------------------------------- */

bool zb_parse_u32(const uint8_t *value, size_t len, uint32_t *out)
{
    if (len == 0) {
        return false;
    }
    uint32_t res = 0;
    for (size_t i = 0; i < len; ++i) {
        uint32_t c = (uint32_t)(value[i] - '0');
        if (value[i] < '0' || value[i] > '9' || res > (UINT32_MAX - c) / 10) {
            return false;
        }
        res = res * 10 + c;
    }
    *out = res;
    return true;
}

int zb_hex_value(char c)
{
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
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

bool zb_starts_with(const char *s, size_t len, const char *prefix)
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
    if (zb_starts_with(endpoint.data, endpoint.len, "https://")) {
        rest = endpoint.data + 8;
        rest_len = endpoint.len - 8;
    } else if (zb_starts_with(endpoint.data, endpoint.len, "http://")) {
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

bool zb_url_split(const char *endpoint, bool *out_tls, char **out_host,
                  uint16_t *out_port)
{
    bool tls = zb_starts_with(endpoint, strlen(endpoint), "https://");
    const char *host = endpoint + (tls ? 8 : 7);
    const char *colon = strchr(host, ':');
    size_t host_len = colon ? (size_t)(colon - host) : strlen(host);
    uint32_t port = tls ? 443 : 80;
    if (colon &&
        !zb_parse_u32((const uint8_t *)colon + 1, strlen(colon + 1), &port)) {
        return false;
    }
    char *copy = zb_strndup(host, host_len);
    if (copy == NULL) {
        return false;
    }
    *out_tls = tls;
    *out_host = copy;
    *out_port = (uint16_t)port;
    return true;
}

/* ---- owned-memory helpers ---------------------------------------------- */

void zb_copy_bounded(uint8_t *buffer, size_t size, size_t *used,
                     const uint8_t *data, size_t len, size_t *consumed)
{
    size_t take = size - *used;
    if (take > len - *consumed) {
        take = len - *consumed;
    }
    if (take != 0) {
        memcpy(buffer + *used, data + *consumed, take);
        *used += take;
        *consumed += take;
    }
}

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
