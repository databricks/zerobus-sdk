/* Connection tests over local sockets, plaintext and TLS. */
#define _GNU_SOURCE

#include <openssl/ssl.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "http_server.h"
#include "internal/network/connection.h"
#include "loopback.h"
#include "test_common.h"

#define CERT_FILE ZB_TEST_DIR "/cert.pem"
#define KEY_FILE ZB_TEST_DIR "/key.pem"
#define REQUEST "POST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n"
#define REPLY "HTTP/1.1 204 No Content\r\n\r\n"

enum {
    TIMEOUT_MS = 5000,
    SHORT_TIMEOUT_MS = 50,
    MAX_QUEUED = 16, /* More than a loopback listener's backlog holds. */
};

/* ---- helpers ----------------------------------------------------------- */

/* Sends REQUEST, then reads the reply, which must be REPLY's length. Reads
 * the first byte alone, after which TLS holds the rest of its record. */
static zerobus_status_t exchange(zb_connection_t *connection,
                                 zb_wakeup_t *wakeup, char *reply)
{
    zb_deadline_t deadline = zb_deadline_after_ms(TIMEOUT_MS);
    size_t sent = 0, received = 0;
    zerobus_status_t status = ZEROBUS_STATUS_OK;
    while (status == ZEROBUS_STATUS_OK && sent < strlen(REQUEST)) {
        zb_io_result_t result = {0};
        status =
            zb_connection_write(connection, (const uint8_t *)REQUEST + sent,
                                strlen(REQUEST) - sent, &result);
        sent += result.bytes;
        if (status == ZEROBUS_STATUS_OK && result.want) {
            status =
                zb_connection_wait(connection, result.want, wakeup, deadline);
        }
    }
    while (status == ZEROBUS_STATUS_OK && received < strlen(REPLY)) {
        zb_io_result_t result = {0};
        status = zb_connection_read(connection, (uint8_t *)reply + received,
                                    received ? strlen(REPLY) - received : 1,
                                    &result);
        if (received == 0 && result.bytes == 1) {
            CHECK(zb_connection_pending(connection) ==
                  (connection->tls != NULL));
        }
        received += result.bytes;
        if (status == ZEROBUS_STATUS_OK && result.eof) {
            status = ZEROBUS_STATUS_UNAVAILABLE;
        } else if (status == ZEROBUS_STATUS_OK && result.want) {
            status =
                zb_connection_wait(connection, result.want, wakeup, deadline);
        }
    }
    reply[received] = '\0';
    return status;
}

/* Opens a connection to the server and runs one exchange. */
static zerobus_status_t open_and_exchange(const http_server_t *server,
                                          const zb_tls_ctx_t *tls,
                                          zb_wakeup_t *wakeup, char *reply)
{
    zb_connection_t connection;
    zerobus_status_t status =
        zb_connection_open(&connection, tls, "127.0.0.1", server->port, false,
                           wakeup, zb_deadline_after_ms(TIMEOUT_MS));
    if (status == ZEROBUS_STATUS_OK) {
        status = exchange(&connection, wakeup, reply);
    }
    zb_connection_close(&connection);
    return status;
}

/* ---- tests ------------------------------------------------------------- */

static void test_wait(void)
{
    zb_wakeup_t *wakeup = NULL;
    int pair[2] = {-1, -1};
    REQUIRE_OK(zb_wakeup_new(&wakeup));
    REQUIRE(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, pair) == 0);
    zb_connection_t connection = {pair[0], NULL};

    CHECK_EQ_INT(zb_connection_wait(&connection, ZB_NET_READ, wakeup,
                                    zb_deadline_after_ms(SHORT_TIMEOUT_MS)),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    REQUIRE_OK(zb_wakeup_signal(wakeup));
    CHECK_EQ_INT(zb_connection_wait(&connection, ZB_NET_READ, wakeup,
                                    ZB_DEADLINE_INFINITE),
                 ZEROBUS_STATUS_CANCELLED);
    REQUIRE_OK(zb_wakeup_drain(wakeup));
    REQUIRE(write(pair[1], "x", 1) == 1);
    CHECK_OK(zb_connection_wait(&connection, ZB_NET_READ, wakeup,
                                ZB_DEADLINE_INFINITE));

    /* A signal wins over a socket that is ready too, which wins over a passed
     * deadline. */
    REQUIRE_OK(zb_wakeup_signal(wakeup));
    CHECK_EQ_INT(zb_connection_wait(&connection, ZB_NET_READ, wakeup,
                                    ZB_DEADLINE_INFINITE),
                 ZEROBUS_STATUS_CANCELLED);
    REQUIRE_OK(zb_wakeup_drain(wakeup));
    CHECK_OK(zb_connection_wait(&connection, ZB_NET_READ, wakeup,
                                ZB_DEADLINE_IMMEDIATE));

zb_cleanup:
    for (int i = 0; i < 2; ++i) {
        if (pair[i] >= 0) {
            close(pair[i]);
        }
    }
    zb_wakeup_free(wakeup);
}

static void test_exchange(void)
{
    zb_wakeup_t *wakeup = NULL;
    zb_tls_ctx_t *tls = NULL;
    char reply[sizeof(REPLY)];
    http_server_t plain = {.reply = {REPLY}};
    http_server_t secure = {
        .reply = {REPLY},
        .tls = loopback_server_tls(CERT_FILE, KEY_FILE, false)};
    REQUIRE(secure.tls != NULL);
    REQUIRE_OK(zb_wakeup_new(&wakeup));
    REQUIRE_OK(zb_tls_ctx_new(CERT_FILE, &tls));

    REQUIRE(http_server_start(&plain));
    CHECK_OK(open_and_exchange(&plain, NULL, wakeup, reply));
    http_server_stop(&plain);
    CHECK(strcmp(reply, REPLY) == 0);
    CHECK(strcmp(plain.request, REQUEST) == 0);

    REQUIRE(http_server_start(&secure));
    CHECK_OK(open_and_exchange(&secure, tls, wakeup, reply));
    http_server_stop(&secure);
    CHECK(strcmp(reply, REPLY) == 0);
    CHECK(strcmp(secure.request, REQUEST) == 0);

zb_cleanup:
    SSL_CTX_free(secure.tls);
    zb_tls_ctx_free(tls);
    zb_wakeup_free(wakeup);
}

static void test_open_failures(void)
{
    zb_wakeup_t *wakeup = NULL;
    zb_tls_ctx_t *tls = NULL;
    zb_tls_ctx_t *system_roots = NULL;
    zb_connection_t connection;
    http_server_t server = {
        .hold = true, .tls = loopback_server_tls(CERT_FILE, KEY_FILE, false)};
    http_server_t plain = {.hold = true};
    uint16_t port = 0;
    REQUIRE(server.tls != NULL);
    REQUIRE_OK(zb_wakeup_new(&wakeup));
    REQUIRE_OK(zb_tls_ctx_new(CERT_FILE, &tls));
    REQUIRE_OK(zb_tls_ctx_new(NULL, &system_roots));
    zb_deadline_t deadline = zb_deadline_after_ms(TIMEOUT_MS);

    /* Nothing listens on the port. */
    int listener = loopback_listen(&port);
    REQUIRE(listener >= 0);
    close(listener);
    CHECK_EQ_INT(zb_connection_open(&connection, NULL, "127.0.0.1", port, false,
                                    wakeup, deadline),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(connection.socket == ZB_SOCKET_INVALID && connection.tls == NULL);

    /* A label over 63 characters fails inside libc, before any DNS query. */
    char host[80];
    memset(host, 'a', 64);
    strcpy(host + 64, ".invalid");
    CHECK_EQ_INT(zb_connection_open(&connection, NULL, host, 443, false, wakeup,
                                    deadline),
                 ZEROBUS_STATUS_UNAVAILABLE);

    /* The server's certificate is not among the system's roots. */
    REQUIRE(http_server_start(&server));
    CHECK_EQ_INT(zb_connection_open(&connection, system_roots, "127.0.0.1",
                                    server.port, false, wakeup, deadline),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(connection.socket == ZB_SOCKET_INVALID && connection.tls == NULL);
    http_server_stop(&server);

    /* A plaintext server never answers the TLS handshake. */
    REQUIRE(http_server_start(&plain));
    CHECK_EQ_INT(zb_connection_open(&connection, tls, "127.0.0.1", plain.port,
                                    false, wakeup,
                                    zb_deadline_after_ms(SHORT_TIMEOUT_MS)),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    http_server_stop(&plain);

    CHECK_EQ_INT(zb_connection_open(&connection, NULL, "127.0.0.1", port, false,
                                    NULL, deadline),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);

zb_cleanup:
    SSL_CTX_free(server.tls);
    zb_tls_ctx_free(system_roots);
    zb_tls_ctx_free(tls);
    zb_wakeup_free(wakeup);
}

/* Nothing accepts, so connections fill the listener's backlog until the next
 * TCP connect stalls. Open then ends at its deadline, or once the wakeup is
 * signalled, with the connection closed. */
static void test_stalled_connect(void)
{
    zb_wakeup_t *wakeup = NULL;
    zb_connection_t queued[MAX_QUEUED];
    zb_connection_t connection;
    int queued_count = 0;
    uint16_t port = 0;
    int listener = loopback_listen(&port);
    REQUIRE(listener >= 0);
    REQUIRE_OK(zb_wakeup_new(&wakeup));

    /* A deadline that passed during the DNS lookup ends the open before it
     * connects, although this connect would complete. */
    CHECK_EQ_INT(zb_connection_open(&connection, NULL, "127.0.0.1", port, false,
                                    wakeup, ZB_DEADLINE_IMMEDIATE),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK(connection.socket == ZB_SOCKET_INVALID);

    zerobus_status_t status;
    do {
        status =
            zb_connection_open(&connection, NULL, "127.0.0.1", port, false,
                               wakeup, zb_deadline_after_ms(SHORT_TIMEOUT_MS));
        if (status == ZEROBUS_STATUS_OK) {
            queued[queued_count++] = connection;
        }
    } while (status == ZEROBUS_STATUS_OK && queued_count < MAX_QUEUED);
    CHECK_EQ_INT(status, ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK(connection.socket == ZB_SOCKET_INVALID);

    REQUIRE_OK(zb_wakeup_signal(wakeup));
    CHECK_EQ_INT(zb_connection_open(&connection, NULL, "127.0.0.1", port, false,
                                    wakeup, zb_deadline_after_ms(TIMEOUT_MS)),
                 ZEROBUS_STATUS_CANCELLED);
    CHECK(connection.socket == ZB_SOCKET_INVALID);

zb_cleanup:
    for (int i = 0; i < queued_count; ++i) {
        zb_connection_close(&queued[i]);
    }
    if (listener >= 0) {
        close(listener);
    }
    zb_wakeup_free(wakeup);
}

int main(void)
{
    if (!loopback_write_certificate(CERT_FILE, KEY_FILE)) {
        fprintf(stderr, "  FAIL: could not write the test certificate\n");
        return 1;
    }
    test_wait();
    test_exchange();
    test_open_failures();
    test_stalled_connect();
    TEST_MAIN_RETURN();
}
