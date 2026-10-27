/* Network platform tests over loopback TCP and local socket pairs only. */
#define _GNU_SOURCE

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "internal/platform/network.h"
#include "loopback.h"
#include "test_common.h"

enum { WAIT_TIMEOUT_MS = 5000, SHORT_TIMEOUT_MS = 50 };

/* ---- helpers ----------------------------------------------------------- */

/* Connects to the next address the way the I/O owner does: start, wait for
 * WRITE, then finish. The caller closes *out_socket if it was set. */
static zerobus_status_t connect_one(zb_net_addresses_t *addresses,
                                    zb_wakeup_t *wakeup,
                                    zb_socket_t *out_socket)
{
    bool connected = false;
    zerobus_status_t status =
        zb_socket_connect_next(addresses, out_socket, &connected);
    if (status == ZEROBUS_STATUS_OK && !connected) {
        zb_net_interest_t interest = {*out_socket, ZB_NET_WRITE,
                                      zb_deadline_after_ms(WAIT_TIMEOUT_MS)};
        unsigned events = 0;
        status = zb_net_wait(&interest, wakeup, &events);
        /* Nothing signals this wakeup, so no events means the deadline passed
         * while the connection was still in progress. */
        if (status == ZEROBUS_STATUS_OK && events == 0) {
            status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        }
        if (status == ZEROBUS_STATUS_OK) {
            status = zb_socket_finish_connect(*out_socket);
        }
    }
    return status;
}

/* ---- tests ------------------------------------------------------------- */

static void test_arguments(void)
{
    zb_net_addresses_t *addresses = NULL;
    zb_socket_t connection = ZB_SOCKET_INVALID;
    zb_net_interest_t interest = {ZB_SOCKET_INVALID, 0, ZB_DEADLINE_IMMEDIATE};
    zb_io_result_t io;
    uint8_t byte = 0;
    unsigned events = 0;
    bool connected = false;

    CHECK_EQ_INT(zb_net_resolve(NULL, 443, &addresses),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_net_resolve("", 443, &addresses),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_net_resolve("localhost", 0, &addresses),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_socket_connect_next(NULL, &connection, &connected),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_socket_finish_connect(ZB_SOCKET_INVALID),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK_EQ_INT(zb_socket_read(ZB_SOCKET_INVALID, &byte, 1, &io),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_socket_write(ZB_SOCKET_INVALID, &byte, 1, &io),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_wakeup_new(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_wakeup_signal(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_wakeup_drain(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(!zb_wakeup_pending(NULL));
    CHECK_EQ_INT(zb_net_wait(&interest, NULL, &events),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zb_net_addresses_free(NULL);
    zb_socket_close(ZB_SOCKET_INVALID);
    zb_wakeup_free(NULL);
}

static void test_resolve_name(void)
{
    zb_net_addresses_t *addresses = NULL;
    CHECK_OK(zb_net_resolve("localhost", 443, &addresses));
    CHECK(addresses != NULL);
    zb_net_addresses_free(addresses);
    addresses = NULL;

    /* A label over 63 characters fails inside libc, before any DNS query. */
    char host[80];
    memset(host, 'a', 64);
    strcpy(host + 64, ".invalid");
    CHECK_EQ_INT(zb_net_resolve(host, 443, &addresses),
                 ZEROBUS_STATUS_UNAVAILABLE);
    CHECK(addresses == NULL);
}

static void test_connect(void)
{
    zb_wakeup_t *wakeup = NULL;
    zb_net_addresses_t *addresses = NULL;
    zb_socket_t connection = ZB_SOCKET_INVALID;
    zb_socket_t refused = ZB_SOCKET_INVALID;
    uint16_t port = 0;
    int listener = loopback_listen(&port);
    int nodelay = 0;
    socklen_t len = sizeof(nodelay);
    REQUIRE(listener >= 0);
    REQUIRE_OK(zb_wakeup_new(&wakeup));
    REQUIRE_OK(zb_net_resolve("127.0.0.1", port, &addresses));

    REQUIRE_OK(connect_one(addresses, wakeup, &connection));
    CHECK(fcntl(connection, F_GETFL) & O_NONBLOCK);
    CHECK(fcntl(connection, F_GETFD) & FD_CLOEXEC);
    CHECK(getsockopt(connection, IPPROTO_TCP, TCP_NODELAY, &nodelay, &len) ==
              0 &&
          nodelay);
    /* 127.0.0.1 has one address, so the cursor has nothing left to try. */
    CHECK_EQ_INT(connect_one(addresses, wakeup, &refused),
                 ZEROBUS_STATUS_UNAVAILABLE);

    /* Linux refuses TCP to broadcast inside connect, before sending. */
    zb_net_addresses_free(addresses);
    addresses = NULL;
    REQUIRE_OK(zb_net_resolve("255.255.255.255", port, &addresses));
    CHECK_EQ_INT(connect_one(addresses, wakeup, &refused),
                 ZEROBUS_STATUS_UNAVAILABLE);

    /* Nothing listens on the port once the listener is closed. */
    close(listener);
    listener = -1;
    zb_net_addresses_free(addresses);
    addresses = NULL;
    REQUIRE_OK(zb_net_resolve("127.0.0.1", port, &addresses));
    CHECK_EQ_INT(connect_one(addresses, wakeup, &refused),
                 ZEROBUS_STATUS_UNAVAILABLE);

zb_cleanup:
    zb_socket_close(refused);
    zb_socket_close(connection);
    zb_net_addresses_free(addresses);
    zb_wakeup_free(wakeup);
    if (listener >= 0) {
        close(listener);
    }
}

static void test_read_write(void)
{
    int pair[2] = {-1, -1};
    uint8_t buffer[16];
    zb_io_result_t io;
    REQUIRE(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, pair) == 0);

    CHECK_OK(zb_socket_read(pair[0], buffer, sizeof(buffer), &io));
    CHECK(io.bytes == 0 && io.want == ZB_NET_READ && !io.eof);

    CHECK_OK(zb_socket_write(pair[1], (const uint8_t *)"hello", 5, &io));
    CHECK(io.bytes == 5 && io.want == 0);
    CHECK_OK(zb_socket_read(pair[0], buffer, sizeof(buffer), &io));
    CHECK(io.bytes == 5 && memcmp(buffer, "hello", 5) == 0);

    close(pair[1]);
    pair[1] = -1;
    CHECK_OK(zb_socket_read(pair[0], buffer, sizeof(buffer), &io));
    CHECK(io.bytes == 0 && io.want == 0 && io.eof);
    /* Without MSG_NOSIGNAL, SIGPIPE would end the test here. */
    CHECK_EQ_INT(zb_socket_write(pair[0], buffer, 1, &io),
                 ZEROBUS_STATUS_UNAVAILABLE);

zb_cleanup:
    zb_socket_close(pair[0]);
    zb_socket_close(pair[1]);
}

static void test_full_socket(void)
{
    static uint8_t buffer[65536];
    int pair[2] = {-1, -1};
    zb_io_result_t io = {0};
    REQUIRE(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, pair) == 0);

    /* Nobody reads pair[1], so its buffer fills within a few writes. */
    for (int i = 0; i < 1000 && io.want == 0; i++) {
        REQUIRE_OK(zb_socket_write(pair[0], buffer, sizeof(buffer), &io));
    }
    CHECK(io.bytes == 0 && io.want == ZB_NET_WRITE);

    /* Closing with that data unread resets the connection. */
    close(pair[1]);
    pair[1] = -1;
    CHECK_EQ_INT(zb_socket_read(pair[0], buffer, sizeof(buffer), &io),
                 ZEROBUS_STATUS_UNAVAILABLE);

zb_cleanup:
    zb_socket_close(pair[0]);
    zb_socket_close(pair[1]);
}

static void test_wait_socket(void)
{
    zb_wakeup_t *wakeup = NULL;
    int pair[2] = {-1, -1};
    unsigned events = 0;
    REQUIRE_OK(zb_wakeup_new(&wakeup));
    REQUIRE(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, pair) == 0);
    zb_net_interest_t interest = {pair[0], ZB_NET_READ | ZB_NET_WRITE,
                                  ZB_DEADLINE_IMMEDIATE};

    CHECK_OK(zb_net_wait(&interest, wakeup, &events));
    CHECK_EQ_INT(events, ZB_NET_WRITE);
    REQUIRE(write(pair[1], "x", 1) == 1);
    CHECK_OK(zb_net_wait(&interest, wakeup, &events));
    CHECK_EQ_INT(events, ZB_NET_READ | ZB_NET_WRITE);
    /* Without requested events the socket is not polled at all. */
    interest.events = 0;
    CHECK_OK(zb_net_wait(&interest, wakeup, &events));
    CHECK_EQ_INT(events, 0);

    close(pair[1]);
    pair[1] = -1;
    interest.events = ZB_NET_READ;
    CHECK_OK(zb_net_wait(&interest, wakeup, &events));
    CHECK(events & ZB_NET_ERROR);

zb_cleanup:
    zb_socket_close(pair[0]);
    zb_socket_close(pair[1]);
    zb_wakeup_free(wakeup);
}

static void test_wakeup(void)
{
    zb_wakeup_t *wakeup = NULL;
    zb_net_interest_t idle = {ZB_SOCKET_INVALID, 0, ZB_DEADLINE_IMMEDIATE};
    unsigned events = 1;
    REQUIRE_OK(zb_wakeup_new(&wakeup));

    CHECK_OK(zb_net_wait(&idle, wakeup, &events));
    CHECK_EQ_INT(events, 0);
    idle.deadline = zb_deadline_after_ms(SHORT_TIMEOUT_MS);
    CHECK_OK(zb_net_wait(&idle, wakeup, &events));
    CHECK(zb_deadline_expired(idle.deadline));

    /* Signals coalesce: they end even an infinite wait, and one drain clears
     * them all. */
    CHECK(!zb_wakeup_pending(wakeup));
    CHECK_OK(zb_wakeup_signal(wakeup));
    CHECK_OK(zb_wakeup_signal(wakeup));
    CHECK(zb_wakeup_pending(wakeup));
    idle.deadline = ZB_DEADLINE_INFINITE;
    CHECK_OK(zb_net_wait(&idle, wakeup, &events));
    CHECK(zb_wakeup_pending(wakeup));
    CHECK_OK(zb_wakeup_drain(wakeup));
    CHECK(!zb_wakeup_pending(wakeup));
    idle.deadline = zb_deadline_after_ms(SHORT_TIMEOUT_MS);
    CHECK_OK(zb_net_wait(&idle, wakeup, &events));
    CHECK(zb_deadline_expired(idle.deadline));

    /* A signal after the drain must still end the next wait. */
    CHECK_OK(zb_wakeup_signal(wakeup));
    idle.deadline = zb_deadline_after_ms(WAIT_TIMEOUT_MS);
    CHECK_OK(zb_net_wait(&idle, wakeup, &events));
    CHECK(!zb_deadline_expired(idle.deadline));

zb_cleanup:
    zb_wakeup_free(wakeup);
}

int main(void)
{
    test_arguments();
    test_resolve_name();
    test_connect();
    test_read_write();
    test_full_socket();
    test_wait_socket();
    test_wakeup();
    TEST_MAIN_RETURN();
}
