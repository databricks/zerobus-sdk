#include "internal/network/connection.h"

/* Connects the socket to the first of addresses that accepts. */
static zerobus_status_t connect_socket(zb_connection_t *connection,
                                       zb_net_addresses_t *addresses,
                                       zb_wakeup_t *wakeup,
                                       zb_deadline_t deadline)
{
    for (;;) {
        bool connected = false;
        zerobus_status_t status =
            zb_socket_connect_next(addresses, &connection->socket, &connected);
        if (status != ZEROBUS_STATUS_OK || connected) {
            return status;
        }
        status = zb_connection_wait(connection, ZB_NET_WRITE, wakeup, deadline);
        if (status == ZEROBUS_STATUS_OK) {
            status = zb_socket_finish_connect(connection->socket);
        }
        if (status == ZEROBUS_STATUS_OK) {
            return status;
        }
        zb_socket_close(connection->socket);
        connection->socket = ZB_SOCKET_INVALID;
        if (status == ZEROBUS_STATUS_DEADLINE_EXCEEDED ||
            status == ZEROBUS_STATUS_CANCELLED) {
            return status;
        }
    }
}

static zerobus_status_t handshake(zb_connection_t *connection,
                                  const zb_tls_ctx_t *tls, const char *host,
                                  bool http2, zb_wakeup_t *wakeup,
                                  zb_deadline_t deadline)
{
    zerobus_status_t status =
        zb_tls_new(tls, connection->socket, host, http2, &connection->tls);
    bool ready = false;
    while (status == ZEROBUS_STATUS_OK && !ready) {
        unsigned want = 0;
        status = zb_tls_handshake(connection->tls, &want, &ready);
        if (status == ZEROBUS_STATUS_OK && !ready) {
            status = zb_connection_wait(connection, want, wakeup, deadline);
        }
    }
    return status;
}

zerobus_status_t zb_connection_wait(const zb_connection_t *connection,
                                    unsigned want, zb_wakeup_t *wakeup,
                                    zb_deadline_t deadline)
{
    zb_net_interest_t interest = {connection->socket, want, deadline};
    unsigned events = 0;
    zerobus_status_t status = zb_net_wait(&interest, wakeup, &events);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    /* A signal wins over a socket that is ready too, which wins over the
     * deadline. Without a signal or a ready socket, the deadline ended the
     * wait. */
    if (zb_wakeup_pending(wakeup)) {
        return ZEROBUS_STATUS_CANCELLED;
    }
    return events ? ZEROBUS_STATUS_OK : ZEROBUS_STATUS_DEADLINE_EXCEEDED;
}

zerobus_status_t zb_connection_open(zb_connection_t *connection,
                                    const zb_tls_ctx_t *tls, const char *host,
                                    uint16_t port, bool http2,
                                    zb_wakeup_t *wakeup, zb_deadline_t deadline)
{
    if (!connection || !wakeup) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    *connection = ZB_CONNECTION_CLOSED;
    zb_net_addresses_t *addresses = NULL;
    zerobus_status_t status = zb_net_resolve(host, port, &addresses);
    /* The lookup is not bounded, so the deadline may have passed during it. */
    if (status == ZEROBUS_STATUS_OK && zb_deadline_expired(deadline)) {
        status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = connect_socket(connection, addresses, wakeup, deadline);
    }
    zb_net_addresses_free(addresses);
    if (status == ZEROBUS_STATUS_OK && tls) {
        status = handshake(connection, tls, host, http2, wakeup, deadline);
    }
    if (status != ZEROBUS_STATUS_OK) {
        zb_connection_close(connection);
    }
    return status;
}

zerobus_status_t zb_connection_read(zb_connection_t *connection, uint8_t *data,
                                    size_t len, zb_io_result_t *out)
{
    return connection->tls ? zb_tls_read(connection->tls, data, len, out)
                           : zb_socket_read(connection->socket, data, len, out);
}

zerobus_status_t zb_connection_write(zb_connection_t *connection,
                                     const uint8_t *data, size_t len,
                                     zb_io_result_t *out)
{
    return connection->tls
               ? zb_tls_write(connection->tls, data, len, out)
               : zb_socket_write(connection->socket, data, len, out);
}

bool zb_connection_pending(const zb_connection_t *connection)
{
    return zb_tls_pending(connection->tls);
}

void zb_connection_close(zb_connection_t *connection)
{
    zb_tls_free(connection->tls);
    zb_socket_close(connection->socket);
    *connection = ZB_CONNECTION_CLOSED;
}
