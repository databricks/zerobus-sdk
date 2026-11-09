#define _GNU_SOURCE

#include "loopback.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

int loopback_listen(uint16_t *out_port)
{
    struct sockaddr_in address = {.sin_family = AF_INET,
                                  .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t len = sizeof(address);
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        return -1;
    }
    if (bind(listener, (struct sockaddr *)&address, len) != 0 ||
        listen(listener, 4) != 0 ||
        getsockname(listener, (struct sockaddr *)&address, &len) != 0) {
        close(listener);
        return -1;
    }
    *out_port = ntohs(address.sin_port);
    return listener;
}

bool loopback_write_certificate(const char *cert_file, const char *key_file)
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
             (key_out = BIO_new_file(key_file, "w")) &&
             (cert_out = BIO_new_file(cert_file, "w")) &&
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

SSL_CTX *loopback_server_tls(const char *cert_file, const char *key_file,
                             bool h2)
{
    SSL_CTX *tls = SSL_CTX_new(TLS_server_method());
    if (tls &&
        (SSL_CTX_use_certificate_file(tls, cert_file, SSL_FILETYPE_PEM) != 1 ||
         SSL_CTX_use_PrivateKey_file(tls, key_file, SSL_FILETYPE_PEM) != 1)) {
        SSL_CTX_free(tls);
        return NULL;
    }
    if (tls && h2) {
        SSL_CTX_set_alpn_select_cb(tls, select_h2, NULL);
    }
    return tls;
}

void loopback_block_sigpipe(void)
{
    /* SIGPIPE goes to the thread that writes. */
    sigset_t blocked;
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGPIPE);
    (void)pthread_sigmask(SIG_BLOCK, &blocked, NULL);
}
