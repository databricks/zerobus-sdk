#include "internal/network/tls.h"

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "internal/platform/log.h"

struct zb_tls_ctx {
    /* Reference counted. */
    SSL_CTX *context;
};

struct zb_tls {
    SSL *session;
    zb_socket_t socket;
    zerobus_status_t io_failure;
    bool http2;
};

/* Writes encrypted data to the socket. */
static int socket_bio_write(BIO *bio, const char *data, int len)
{
    zb_tls_t *tls = BIO_get_data(bio);
    BIO_clear_retry_flags(bio);
    if (!data || len <= 0) {
        return 0;
    }
    zb_io_result_t result;
    tls->io_failure = zb_socket_write(tls->socket, (const uint8_t *)data,
                                      (size_t)len, &result);
    if (tls->io_failure != ZEROBUS_STATUS_OK) {
        return -1;
    }
    if (result.want) {
        BIO_set_retry_write(bio);
        return -1;
    }
    return (int)result.bytes;
}

/* Reads decrypted data from the socket. */
static int socket_bio_read(BIO *bio, char *data, int len)
{
    zb_tls_t *tls = BIO_get_data(bio);
    BIO_clear_retry_flags(bio);
    if (!data || len <= 0) {
        return 0;
    }
    zb_io_result_t result;
    tls->io_failure =
        zb_socket_read(tls->socket, (uint8_t *)data, (size_t)len, &result);
    if (tls->io_failure != ZEROBUS_STATUS_OK) {
        return -1;
    }
    if (result.want) {
        BIO_set_retry_read(bio);
        return -1;
    }
    return (int)result.bytes;
}

static long socket_bio_ctrl(BIO *bio, int command, long number, void *pointer)
{
    (void)bio;
    (void)number;
    (void)pointer;
    return command == BIO_CTRL_FLUSH;
}

/* BIO type indexes come from a small process-wide counter, so every session
 * shares one method. */
static zb_once_t socket_bio_once = ZB_ONCE_INIT;
static BIO_METHOD *socket_bio_method;

static void socket_bio_init(void)
{
    int index = BIO_get_new_index();
    if (index < 0) {
        return;
    }
    BIO_METHOD *method = BIO_meth_new(BIO_TYPE_SOURCE_SINK | index,
                                      "zerobus nonblocking socket");
    if (method && (BIO_meth_set_write(method, socket_bio_write) != 1 ||
                   BIO_meth_set_read(method, socket_bio_read) != 1 ||
                   BIO_meth_set_ctrl(method, socket_bio_ctrl) != 1)) {
        BIO_meth_free(method);
        method = NULL;
    }
    socket_bio_method = method;
}

/* Writes OpenSSL's reason for a failure into reason, then clears its error
 * queue. */
static void take_reason(const SSL *session, char *reason, size_t size)
{
    char error[128];
    /* Socket failures and closes leave no reason in the queue. */
    const char *cause = session ? "connection failed" : "failed";
    if (ERR_peek_error()) {
        ERR_error_string_n(ERR_peek_error(), error, sizeof(error));
        cause = error;
    }
    long verify = session ? SSL_get_verify_result(session) : X509_V_OK;
    if (verify != X509_V_OK) {
        snprintf(reason, size, "%s (certificate: %s)", cause,
                 X509_verify_cert_error_string(verify));
    } else {
        snprintf(reason, size, "%s", cause);
    }
    ERR_clear_error();
}

/* Logs the failure's reason as the calling function. */
#define LOG_FAILURE(level, session)                                            \
    do {                                                                       \
        char reason[256];                                                      \
        take_reason(session, reason, sizeof(reason));                          \
        ZB_LOG(level, "%s", reason);                                           \
    } while (0)

zerobus_status_t zb_tls_ctx_new(const char *ca_file, zb_tls_ctx_t **out_ctx)
{
    if (!out_ctx || (ca_file && !ca_file[0])) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zb_tls_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status = ZEROBUS_STATUS_INTERNAL;
    ERR_clear_error();
    ctx->context = SSL_CTX_new(TLS_client_method());
    if (!ctx->context) {
        goto fail;
    }
    SSL_CTX_set_verify(ctx->context, SSL_VERIFY_PEER, NULL);
    if (SSL_CTX_set_min_proto_version(ctx->context, TLS1_2_VERSION) != 1) {
        goto fail;
    }
    SSL_CTX_set_options(ctx->context, SSL_OP_NO_COMPRESSION);
    /* HTTP/2 over TLS 1.2 requires forward-secret AEAD cipher suites. TLS 1.3
     * has its own suitable default suite list, unaffected by this setting. */
    if (SSL_CTX_set_cipher_list(ctx->context,
                                "ECDHE-ECDSA-AES128-GCM-SHA256:"
                                "ECDHE-RSA-AES128-GCM-SHA256:"
                                "ECDHE-ECDSA-AES256-GCM-SHA384:"
                                "ECDHE-RSA-AES256-GCM-SHA384:"
                                "ECDHE-ECDSA-CHACHA20-POLY1305:"
                                "ECDHE-RSA-CHACHA20-POLY1305") != 1) {
        goto fail;
    }
    if (ca_file) {
        if (SSL_CTX_load_verify_locations(ctx->context, ca_file, NULL) != 1) {
            status = ZEROBUS_STATUS_INVALID_ARGUMENT;
            goto fail;
        }
    } else if (SSL_CTX_set_default_verify_paths(ctx->context) != 1) {
        goto fail;
    }
    if (zb_once(&socket_bio_once, socket_bio_init) != ZEROBUS_STATUS_OK ||
        !socket_bio_method) {
        goto fail;
    }
    *out_ctx = ctx;
    return ZEROBUS_STATUS_OK;

fail:
    zb_tls_ctx_free(ctx);
    LOG_FAILURE(ZB_LOG_WARN, NULL);
    return status;
}

void zb_tls_ctx_free(zb_tls_ctx_t *ctx)
{
    if (!ctx) {
        return;
    }
    SSL_CTX_free(ctx->context);
    free(ctx);
}

/* Copies host into name without the DNS root dot at its end, which
 * certificates and SNI leave out. Returns false if it doesn't fit. */
static bool dns_name(const char *host, char *name, size_t size)
{
    size_t len = strlen(host);
    if (len > 1 && host[len - 1] == '.') {
        len--;
    }
    if (len >= size) {
        return false;
    }
    memcpy(name, host, len);
    name[len] = '\0';
    return true;
}

zerobus_status_t zb_tls_new(const zb_tls_ctx_t *ctx, zb_socket_t socket,
                            const char *host, bool http2, zb_tls_t **out_tls)
{
    if (!ctx || socket == ZB_SOCKET_INVALID || !host || !host[0] || !out_tls) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zb_tls_t *tls = calloc(1, sizeof(*tls));
    if (!tls) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    tls->socket = socket;
    tls->http2 = http2;
    ERR_clear_error();
    tls->session = SSL_new(ctx->context);
    if (!tls->session) {
        goto fail;
    }

    BIO *bio = BIO_new(socket_bio_method);
    if (!bio) {
        goto fail;
    }
    BIO_set_data(bio, tls);
    BIO_set_init(bio, 1);
    SSL_set_bio(tls->session, bio, bio);
    SSL_set_connect_state(tls->session);
    SSL_set_mode(tls->session, SSL_MODE_ENABLE_PARTIAL_WRITE);
    if (http2) {
        SSL_set_options(tls->session, SSL_OP_NO_RENEGOTIATION);
        const unsigned char alpn[] = {2, 'h', '2'};
        if (SSL_set_alpn_protos(tls->session, alpn, sizeof(alpn)) != 0) {
            goto fail;
        }
    }

    /* Set host verification parameters. */
    X509_VERIFY_PARAM *verify = SSL_get0_param(tls->session);
    X509_VERIFY_PARAM_set_hostflags(verify,
                                    X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
    ASN1_OCTET_STRING *address = a2i_IPADDRESS(host);
    if (address) {
        /* Host is an IP literal. */
        ASN1_OCTET_STRING_free(address);
        if (X509_VERIFY_PARAM_set1_ip_asc(verify, host) != 1) {
            goto fail;
        }
    } else {
        /* Host is a DNS name. */
        char name[256];
        ERR_clear_error();
        if (!dns_name(host, name, sizeof(name)) ||
            SSL_set1_host(tls->session, name) != 1 ||
            SSL_set_tlsext_host_name(tls->session, name) != 1) {
            goto fail;
        }
    }
    *out_tls = tls;
    return ZEROBUS_STATUS_OK;

fail:
    zb_tls_free(tls);
    LOG_FAILURE(ZB_LOG_WARN, NULL);
    return ZEROBUS_STATUS_INTERNAL;
}

void zb_tls_free(zb_tls_t *tls)
{
    if (!tls) {
        return;
    }
    SSL_free(tls->session);
    free(tls);
}

static zerobus_status_t io_result(zb_tls_t *tls, int result, size_t bytes,
                                  zb_io_result_t *out)
{
    *out = (zb_io_result_t){.bytes = bytes};
    if (result == 1) {
        return ZEROBUS_STATUS_OK;
    }
    int ssl_error = SSL_get_error(tls->session, result);
    if (ssl_error == SSL_ERROR_WANT_READ) {
        out->want = ZB_NET_READ;
        return ZEROBUS_STATUS_OK;
    }
    if (ssl_error == SSL_ERROR_WANT_WRITE) {
        out->want = ZB_NET_WRITE;
        return ZEROBUS_STATUS_OK;
    }
    if (ssl_error == SSL_ERROR_ZERO_RETURN) {
        out->eof = true;
        return ZEROBUS_STATUS_OK;
    }
    if (SSL_get_verify_result(tls->session) != X509_V_OK) {
        /* The server's certificate failed, not our credentials. */
        return ZEROBUS_STATUS_UNAVAILABLE;
    }
    if (tls->io_failure != ZEROBUS_STATUS_OK) {
        return tls->io_failure;
    }
    return ZEROBUS_STATUS_UNAVAILABLE;
}

zerobus_status_t zb_tls_handshake(zb_tls_t *tls, unsigned *out_want,
                                  bool *out_ready)
{
    if (!tls || !out_want || !out_ready) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    ERR_clear_error();
    tls->io_failure = ZEROBUS_STATUS_OK;
    zb_io_result_t progress;
    /* Runs the handshake as far as it can without waiting. */
    zerobus_status_t status =
        io_result(tls, SSL_connect(tls->session), 0, &progress);
    if (status != ZEROBUS_STATUS_OK) {
        LOG_FAILURE(ZB_LOG_WARN, tls->session);
        return status;
    }
    if (progress.eof) {
        ZB_WARN("closed by the server");
        return ZEROBUS_STATUS_UNAVAILABLE;
    }
    *out_want = progress.want;
    *out_ready = !progress.want;
    if (*out_ready) {
        const unsigned char *protocol = NULL;
        unsigned len = 0;
        SSL_get0_alpn_selected(tls->session, &protocol, &len);
        if (SSL_get_verify_result(tls->session) != X509_V_OK) {
            LOG_FAILURE(ZB_LOG_WARN, tls->session);
            return ZEROBUS_STATUS_UNAVAILABLE;
        }
        if (tls->http2 && (len != 2 || memcmp(protocol, "h2", 2) != 0)) {
            ZB_WARN("server did not select HTTP/2");
            return ZEROBUS_STATUS_UNAVAILABLE;
        }
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_tls_read(zb_tls_t *tls, uint8_t *data, size_t len,
                             zb_io_result_t *out)
{
    if (!tls || !data || !len || !out) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    ERR_clear_error();
    tls->io_failure = ZEROBUS_STATUS_OK;
    size_t bytes = 0;
    int result = SSL_read_ex(tls->session, data, len, &bytes);
    zerobus_status_t status = io_result(tls, result, bytes, out);
    if (status != ZEROBUS_STATUS_OK) {
        LOG_FAILURE(ZB_LOG_DEBUG, tls->session);
    }
    return status;
}

zerobus_status_t zb_tls_write(zb_tls_t *tls, const uint8_t *data, size_t len,
                              zb_io_result_t *out)
{
    if (!tls || !data || !len || !out) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    ERR_clear_error();
    tls->io_failure = ZEROBUS_STATUS_OK;
    size_t bytes = 0;
    int result = SSL_write_ex(tls->session, data, len, &bytes);
    zerobus_status_t status = io_result(tls, result, bytes, out);
    /* After the peer's close_notify, OpenSSL reports any failed write as a
     * clean end. We report failure instead. */
    if (status == ZEROBUS_STATUS_OK && out->eof) {
        out->eof = false;
        status = tls->io_failure != ZEROBUS_STATUS_OK
                     ? tls->io_failure
                     : ZEROBUS_STATUS_UNAVAILABLE;
    }
    if (status != ZEROBUS_STATUS_OK) {
        LOG_FAILURE(ZB_LOG_DEBUG, tls->session);
    }
    return status;
}

bool zb_tls_pending(const zb_tls_t *tls)
{
    return tls && SSL_pending(tls->session) > 0;
}
