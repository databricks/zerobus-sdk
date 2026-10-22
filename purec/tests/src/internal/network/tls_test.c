/* TLS session tests against an in-process OpenSSL server on a socket pair. */
#define _GNU_SOURCE

#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "internal/network/tls.h"
#include "test_common.h"

#define CERT_FILE ZB_TEST_DIR "/cert.pem"
#define KEY_FILE ZB_TEST_DIR "/key.pem"

/* ---- helpers ----------------------------------------------------------- */

/* Writes a new self-signed certificate for localhost and 127.0.0.1 to
 * CERT_FILE, and its key to KEY_FILE. */
static bool write_certificate(void)
{
    EVP_PKEY_CTX *keygen = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, NULL);
    EVP_PKEY *key = NULL;
    X509 *cert = X509_new();
    X509_NAME *name = cert ? X509_get_subject_name(cert) : NULL;
    X509_EXTENSION *names = NULL;
    X509V3_CTX names_ctx;
    BIO *key_out = NULL;
    BIO *cert_out = NULL;

    bool ok = keygen && name && EVP_PKEY_keygen_init(keygen) == 1 &&
              EVP_PKEY_CTX_ctrl_str(keygen, "ec_paramgen_curve", "P-256") > 0 &&
              EVP_PKEY_keygen(keygen, &key) == 1 &&
              X509_set_version(cert, 2) == 1 &&
              ASN1_INTEGER_set(X509_get_serialNumber(cert), 1) == 1 &&
              X509_gmtime_adj(X509_getm_notBefore(cert), 0) &&
              X509_gmtime_adj(X509_getm_notAfter(cert), 24 * 60 * 60) &&
              X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                         (const unsigned char *)"localhost", -1,
                                         -1, 0) == 1 &&
              X509_set_issuer_name(cert, name) == 1 &&
              X509_set_pubkey(cert, key) == 1;
    if (ok) {
        X509V3_set_ctx(&names_ctx, cert, cert, NULL, NULL, 0);
        names = X509V3_EXT_nconf_nid(NULL, &names_ctx, NID_subject_alt_name,
                                     "DNS:localhost,IP:127.0.0.1");
        ok = names && X509_add_ext(cert, names, -1) == 1 &&
             X509_sign(cert, key, EVP_sha256()) > 0 &&
             (key_out = BIO_new_file(KEY_FILE, "w")) &&
             (cert_out = BIO_new_file(CERT_FILE, "w")) &&
             PEM_write_bio_PrivateKey(key_out, key, NULL, NULL, 0, NULL,
                                      NULL) == 1 &&
             PEM_write_bio_X509(cert_out, cert) == 1;
    }
    BIO_free(cert_out);
    BIO_free(key_out);
    X509_EXTENSION_free(names);
    X509_free(cert);
    EVP_PKEY_free(key);
    EVP_PKEY_CTX_free(keygen);
    return ok;
}

static int select_h2(SSL *ssl, const unsigned char **out, unsigned char *outlen,
                     const unsigned char *in, unsigned inlen, void *arg)
{
    (void)ssl;
    (void)arg;
    static const unsigned char h2[] = {2, 'h', '2'};
    return SSL_select_next_proto((unsigned char **)out, outlen, h2, sizeof(h2),
                                 in, inlen) == OPENSSL_NPN_NEGOTIATED
               ? SSL_TLSEXT_ERR_OK
               : SSL_TLSEXT_ERR_NOACK;
}

/* Test server options. Without SERVER_H2, the server selects no protocol. */
enum {
    SERVER_H2 = 1,        /* selects h2 when the client offers it */
    SERVER_TLS12 = 2,     /* speaks at most TLS 1.2 */
    SERVER_TLS12_CBC = 4, /* speaks TLS 1.2 with only a CBC cipher suite */
};

/* Creates a server for socket with the test certificate. */
static SSL *server_new(int socket, unsigned options)
{
    SSL_CTX *context = SSL_CTX_new(TLS_server_method());
    SSL *server = NULL;
    if (context &&
        SSL_CTX_use_certificate_file(context, CERT_FILE, SSL_FILETYPE_PEM) &&
        SSL_CTX_use_PrivateKey_file(context, KEY_FILE, SSL_FILETYPE_PEM)) {
        if (options & SERVER_H2) {
            SSL_CTX_set_alpn_select_cb(context, select_h2, NULL);
        }
        if (options & (SERVER_TLS12 | SERVER_TLS12_CBC)) {
            SSL_CTX_set_max_proto_version(context, TLS1_2_VERSION);
        }
        if (options & SERVER_TLS12_CBC) {
            SSL_CTX_set_cipher_list(context, "ECDHE-ECDSA-AES128-SHA");
        }
        server = SSL_new(context);
    }
    SSL_CTX_free(context);
    if (server) {
        SSL_set_fd(server, socket);
        SSL_set_accept_state(server);
    }
    return server;
}

typedef struct {
    int pair[2];
    SSL *server;
    zb_tls_t *tls;
} conn_t;

/* Connects a session to a test server over a socket pair, running both
 * handshakes on this thread. Returns the session's handshake status. */
static zerobus_status_t conn_open(conn_t *conn, const char *ca_file,
                                  const char *host, bool http2, unsigned server)
{
    *conn = (conn_t){.pair = {-1, -1}};
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, conn->pair) != 0 ||
        !(conn->server = server_new(conn->pair[1], server))) {
        return ZEROBUS_STATUS_INTERNAL;
    }
    zb_tls_ctx_t *ctx = NULL;
    zerobus_status_t status = zb_tls_ctx_new(ca_file, &ctx);
    if (status == ZEROBUS_STATUS_OK) {
        status = zb_tls_new(ctx, conn->pair[0], host, http2, &conn->tls);
    }
    /* Sessions keep the context alive, so it can go before the handshake. */
    zb_tls_ctx_free(ctx);
    bool ready = false;
    for (int i = 0; i < 10 && status == ZEROBUS_STATUS_OK && !ready; i++) {
        unsigned want = 0;
        status = zb_tls_handshake(conn->tls, &want, &ready);
        SSL_do_handshake(conn->server);
    }
    if (status == ZEROBUS_STATUS_OK && !ready) {
        status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
    }
    return status;
}

static void conn_close(conn_t *conn)
{
    zb_tls_free(conn->tls);
    SSL_free(conn->server);
    for (int i = 0; i < 2; i++) {
        if (conn->pair[i] >= 0) {
            close(conn->pair[i]);
        }
    }
    *conn = (conn_t){.pair = {-1, -1}};
}

/* ---- tests ------------------------------------------------------------- */

static void test_arguments(void)
{
    zb_tls_ctx_t *ctx = NULL;
    zb_tls_t *tls = NULL;
    zb_io_result_t io;
    uint8_t byte = 0;
    unsigned want = 0;
    bool ready = false;

    CHECK_EQ_INT(zb_tls_ctx_new(NULL, NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_tls_ctx_new("", &ctx), ZEROBUS_STATUS_INVALID_ARGUMENT);
    /* A CA file must exist and hold at least one certificate. */
    CHECK_EQ_INT(zb_tls_ctx_new(ZB_TEST_DIR "/missing.pem", &ctx),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_tls_ctx_new(KEY_FILE, &ctx),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(ctx == NULL);

    REQUIRE_OK(zb_tls_ctx_new(NULL, &ctx));
    CHECK_EQ_INT(zb_tls_new(NULL, 0, "localhost", false, &tls),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_tls_new(ctx, ZB_SOCKET_INVALID, "localhost", false, &tls),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_tls_new(ctx, 0, NULL, false, &tls),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_tls_new(ctx, 0, "", false, &tls),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_tls_new(ctx, 0, "localhost", false, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_tls_handshake(NULL, &want, &ready),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_tls_read(NULL, &byte, 1, &io),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_tls_write(NULL, &byte, 1, &io),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(!zb_tls_pending(NULL));
    zb_tls_free(NULL);
    zb_tls_ctx_free(NULL);

zb_cleanup:
    zb_tls_ctx_free(ctx);
}

static void test_many_sessions(void)
{
    zb_tls_ctx_t *ctx = NULL;
    REQUIRE_OK(zb_tls_ctx_new(CERT_FILE, &ctx));

    /* Sessions share one BIO method, so they take no IDs from OpenSSL's
     * process-wide BIO type counter: only this test's own call advances it. */
    int before = BIO_get_new_index();
    for (int i = 0; i < 200; i++) {
        zb_tls_t *tls = NULL;
        CHECK_OK(zb_tls_new(ctx, 0, "localhost", false, &tls));
        zb_tls_free(tls);
    }
    CHECK_EQ_INT(BIO_get_new_index(), before + 1);

zb_cleanup:
    zb_tls_ctx_free(ctx);
}

static void test_read_write(void)
{
    conn_t conn = {.pair = {-1, -1}};
    uint8_t buffer[64];
    zb_io_result_t io;
    size_t bytes = 0;
    const unsigned char *protocol = NULL;
    unsigned protocol_len = 1;
    REQUIRE_OK(conn_open(&conn, CERT_FILE, "localhost", false, SERVER_H2));

    /* A DNS host is sent as SNI. */
    const char *name =
        SSL_get_servername(conn.server, TLSEXT_NAMETYPE_host_name);
    CHECK(name && strcmp(name, "localhost") == 0);
    /* Without http2 the session offers no protocol, so none is selected. */
    SSL_get0_alpn_selected(conn.server, &protocol, &protocol_len);
    CHECK_EQ_INT(protocol_len, 0);

    CHECK_OK(zb_tls_write(conn.tls, (const uint8_t *)"hello", 5, &io));
    CHECK(io.bytes == 5 && io.want == 0);
    CHECK(SSL_read_ex(conn.server, buffer, sizeof(buffer), &bytes) == 1);
    CHECK(bytes == 5 && memcmp(buffer, "hello", 5) == 0);

    CHECK_OK(zb_tls_read(conn.tls, buffer, sizeof(buffer), &io));
    CHECK(io.bytes == 0 && io.want == ZB_NET_READ && !io.eof);

    /* Bytes that don't fit stay inside OpenSSL, reported as pending. */
    REQUIRE(SSL_write_ex(conn.server, "world", 5, &bytes) == 1);
    CHECK_OK(zb_tls_read(conn.tls, buffer, 2, &io));
    CHECK(io.bytes == 2 && zb_tls_pending(conn.tls));
    CHECK_OK(zb_tls_read(conn.tls, buffer + 2, sizeof(buffer) - 2, &io));
    CHECK(io.bytes == 3 && !zb_tls_pending(conn.tls));
    CHECK(memcmp(buffer, "world", 5) == 0);

    /* The server's close_notify is a clean end. */
    SSL_shutdown(conn.server);
    CHECK_OK(zb_tls_read(conn.tls, buffer, sizeof(buffer), &io));
    CHECK(io.bytes == 0 && io.eof);
    /* It ends only reading: a write that fails later is still a failure. */
    close(conn.pair[1]);
    conn.pair[1] = -1;
    CHECK_EQ_INT(zb_tls_write(conn.tls, (const uint8_t *)"x", 1, &io),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(!io.eof);

zb_cleanup:
    conn_close(&conn);
}

static void test_closed_connection(void)
{
    conn_t conn = {.pair = {-1, -1}};
    uint8_t buffer[64];
    zb_io_result_t io;
    REQUIRE_OK(conn_open(&conn, CERT_FILE, "localhost", false, 0));

    /* The server's socket closes without a close_notify. Without MSG_NOSIGNAL
     * in the socket wrapper, SIGPIPE would end the test at the write. */
    close(conn.pair[1]);
    conn.pair[1] = -1;
    CHECK_EQ_INT(zb_tls_write(conn.tls, (const uint8_t *)"x", 1, &io),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zb_tls_read(conn.tls, buffer, sizeof(buffer), &io),
                 ZEROBUS_STATUS_UNAVAILABLE);

zb_cleanup:
    conn_close(&conn);
}

static void test_full_socket(void)
{
    static uint8_t data[65536];
    static uint8_t drained[16384];
    conn_t conn = {.pair = {-1, -1}};
    zb_io_result_t io = {0};
    size_t bytes = 0;
    REQUIRE_OK(conn_open(&conn, CERT_FILE, "localhost", false, 0));

    /* The server reads nothing, so the socket fills within a few writes. */
    for (int i = 0; i < 1000 && io.want == 0; i++) {
        REQUIRE_OK(zb_tls_write(conn.tls, data, sizeof(data), &io));
    }
    CHECK(io.bytes == 0 && io.want == ZB_NET_WRITE);

    /* Once the server drains the socket, the same write goes through. */
    while (SSL_read_ex(conn.server, drained, sizeof(drained), &bytes) == 1) {
    }
    CHECK_OK(zb_tls_write(conn.tls, data, sizeof(data), &io));
    CHECK(io.bytes > 0 && io.want == 0);

zb_cleanup:
    conn_close(&conn);
}

static void test_host_check(void)
{
    conn_t conn = {.pair = {-1, -1}};

    /* The certificate names localhost and 127.0.0.1 only. */
    CHECK_EQ_INT(conn_open(&conn, CERT_FILE, "other.example", false, 0),
                 ZEROBUS_STATUS_UNAUTHENTICATED);
    conn_close(&conn);
    CHECK_OK(conn_open(&conn, CERT_FILE, "127.0.0.1", false, 0));
    /* An IP host is not sent as SNI. */
    CHECK(SSL_get_servername(conn.server, TLSEXT_NAMETYPE_host_name) == NULL);
    conn_close(&conn);
    CHECK_EQ_INT(conn_open(&conn, CERT_FILE, "127.0.0.2", false, 0),
                 ZEROBUS_STATUS_UNAUTHENTICATED);
    conn_close(&conn);

    /* The system trust roots don't include the self-signed certificate. */
    CHECK_EQ_INT(conn_open(&conn, NULL, "localhost", false, 0),
                 ZEROBUS_STATUS_UNAUTHENTICATED);
    conn_close(&conn);
    /* SSL_CERT_FILE replaces the system's default roots file. */
    CHECK(setenv("SSL_CERT_FILE", CERT_FILE, 1) == 0);
    CHECK_OK(conn_open(&conn, NULL, "localhost", false, 0));
    conn_close(&conn);
    unsetenv("SSL_CERT_FILE");
}

static void test_alpn(void)
{
    conn_t conn = {.pair = {-1, -1}};
    const unsigned char *protocol = NULL;
    unsigned protocol_len = 0;

    CHECK_OK(conn_open(&conn, CERT_FILE, "localhost", true, SERVER_H2));
    SSL_get0_alpn_selected(conn.server, &protocol, &protocol_len);
    CHECK(protocol_len == 2 && memcmp(protocol, "h2", 2) == 0);
    conn_close(&conn);

    /* A server that selects no protocol does not speak HTTP/2. */
    CHECK_EQ_INT(conn_open(&conn, CERT_FILE, "localhost", true, 0),
                 ZEROBUS_STATUS_UNAVAILABLE);
    conn_close(&conn);
}

static void test_tls12(void)
{
    conn_t conn = {.pair = {-1, -1}};

    /* TLS 1.2 negotiates from our cipher list, which TLS 1.3 ignores. */
    CHECK_OK(conn_open(&conn, CERT_FILE, "localhost", true,
                       SERVER_H2 | SERVER_TLS12));
    CHECK_EQ_INT(SSL_version(conn.server), TLS1_2_VERSION);
    CHECK(SSL_CIPHER_is_aead(SSL_get_current_cipher(conn.server)));
    conn_close(&conn);

    /* Our list has no CBC suites, so this server shares none with us. */
    CHECK_EQ_INT(conn_open(&conn, CERT_FILE, "localhost", true,
                           SERVER_H2 | SERVER_TLS12_CBC),
                 ZEROBUS_STATUS_UNAVAILABLE);
    conn_close(&conn);
}

static void test_handshake_close_notify(void)
{
    /* A plaintext close_notify alert record: type 21, TLS 1.0, length 2,
     * warning level, close_notify. */
    static const uint8_t close_notify[] = {21, 3, 1, 0, 2, 1, 0};
    int pair[2] = {-1, -1};
    zb_tls_ctx_t *ctx = NULL;
    zb_tls_t *tls = NULL;
    unsigned want = 0;
    bool ready = false;
    REQUIRE(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, pair) == 0);
    REQUIRE_OK(zb_tls_ctx_new(CERT_FILE, &ctx));
    REQUIRE_OK(zb_tls_new(ctx, pair[0], "localhost", false, &tls));

    CHECK_OK(zb_tls_handshake(tls, &want, &ready));
    CHECK(!ready && want == ZB_NET_READ);
    /* The peer ends the session before the handshake finishes. */
    REQUIRE(write(pair[1], close_notify, sizeof(close_notify)) ==
            (ssize_t)sizeof(close_notify));
    CHECK_EQ_INT(zb_tls_handshake(tls, &want, &ready),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(!ready);

zb_cleanup:
    zb_tls_free(tls);
    zb_tls_ctx_free(ctx);
    for (int i = 0; i < 2; i++) {
        if (pair[i] >= 0) {
            close(pair[i]);
        }
    }
}

int main(void)
{
    if (!write_certificate()) {
        fprintf(stderr, "  FAIL: could not write the test certificate\n");
        return 1;
    }
    test_arguments();
    test_many_sessions();
    test_read_write();
    test_closed_connection();
    test_full_socket();
    test_host_check();
    test_alpn();
    test_tls12();
    test_handshake_close_notify();
    TEST_MAIN_RETURN();
}
