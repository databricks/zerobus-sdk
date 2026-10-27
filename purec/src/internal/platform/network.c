/* Requires pipe2, SOCK_NONBLOCK, SOCK_CLOEXEC and MSG_NOSIGNAL */
#define _GNU_SOURCE

#include "internal/platform/network.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

struct zb_net_addresses {
    struct addrinfo *head;
    struct addrinfo *cursor;
};

struct zb_wakeup {
    zb_mutex_t mutex;
    int reader;
    int writer;
    bool pending;
};

static zerobus_status_t socket_error(int error)
{
    switch (error) {
    case ENOMEM:
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    case EMFILE:
    case ENFILE:
    case ENOBUFS:
        return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    default:
        return ZEROBUS_STATUS_UNAVAILABLE;
    }
}

zerobus_status_t zb_net_resolve(const char *host, uint16_t port,
                                zb_net_addresses_t **out_addresses)
{
    if (!host || !host[0] || !port || !out_addresses) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }

    zb_net_addresses_t *addresses = calloc(1, sizeof(*addresses));
    if (!addresses) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    struct addrinfo hints = {0};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_NUMERICSERV;
    char service[6];
    snprintf(service, sizeof(service), "%u", (unsigned)port);
    int result = getaddrinfo(host, service, &hints, &addresses->head);
    if (result) {
        zb_net_addresses_free(addresses);
        return result == EAI_MEMORY ? ZEROBUS_STATUS_OUT_OF_MEMORY
                                    : ZEROBUS_STATUS_UNAVAILABLE;
    }
    addresses->cursor = addresses->head;
    *out_addresses = addresses;
    return ZEROBUS_STATUS_OK;
}

void zb_net_addresses_free(zb_net_addresses_t *addresses)
{
    if (!addresses) {
        return;
    }
    if (addresses->head) {
        freeaddrinfo(addresses->head);
    }
    free(addresses);
}

zerobus_status_t zb_socket_connect_next(zb_net_addresses_t *addresses,
                                        zb_socket_t *out_socket,
                                        bool *out_connected)
{
    if (!addresses || !out_socket || !out_connected) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zerobus_status_t status = ZEROBUS_STATUS_UNAVAILABLE;
    while (addresses->cursor) {
        const struct addrinfo *address = addresses->cursor;
        addresses->cursor = address->ai_next;
        int descriptor =
            socket(address->ai_family,
                   address->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                   address->ai_protocol);
        if (descriptor < 0) {
            status = socket_error(errno);
            if (status != ZEROBUS_STATUS_UNAVAILABLE) {
                return status;
            }
            continue;
        }
        int enabled = 1;
        if (setsockopt(descriptor, IPPROTO_TCP, TCP_NODELAY, &enabled,
                       sizeof(enabled)) == 0) {
            int result =
                connect(descriptor, address->ai_addr, address->ai_addrlen);
            if (result == 0 || errno == EINPROGRESS || errno == EINTR) {
                *out_socket = descriptor;
                *out_connected = result == 0;
                return ZEROBUS_STATUS_OK;
            }
        }
        status = socket_error(errno);
        close(descriptor);
    }
    return status;
}

zerobus_status_t zb_socket_finish_connect(zb_socket_t socket)
{
    int error = 0;
    socklen_t len = sizeof(error);
    if (getsockopt(socket, SOL_SOCKET, SO_ERROR, &error, &len) != 0) {
        return socket_error(errno);
    }
    return error ? socket_error(error) : ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_socket_read(zb_socket_t socket, uint8_t *data, size_t len,
                                zb_io_result_t *out)
{
    if (socket == ZB_SOCKET_INVALID || !data || !len || !out) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (len > (size_t)SSIZE_MAX) {
        len = (size_t)SSIZE_MAX;
    }
    ssize_t result;
    do {
        result = recv(socket, data, len, 0);
    } while (result < 0 && errno == EINTR);
    if (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        return socket_error(errno);
    }
    *out = (zb_io_result_t){.bytes = result > 0 ? (size_t)result : 0,
                            .want = result < 0 ? ZB_NET_READ : 0,
                            .eof = result == 0};
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_socket_write(zb_socket_t socket, const uint8_t *data,
                                 size_t len, zb_io_result_t *out)
{
    if (socket == ZB_SOCKET_INVALID || !data || !len || !out) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (len > (size_t)SSIZE_MAX) {
        len = (size_t)SSIZE_MAX;
    }
    ssize_t result;
    do {
        result = send(socket, data, len, MSG_NOSIGNAL);
    } while (result < 0 && errno == EINTR);
    if (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        return socket_error(errno);
    }
    if (result == 0) {
        return ZEROBUS_STATUS_UNAVAILABLE;
    }
    *out = (zb_io_result_t){.bytes = result > 0 ? (size_t)result : 0,
                            .want = result < 0 ? ZB_NET_WRITE : 0};
    return ZEROBUS_STATUS_OK;
}

void zb_socket_close(zb_socket_t socket)
{
    if (socket != ZB_SOCKET_INVALID) {
        close(socket);
    }
}

zerobus_status_t zb_wakeup_new(zb_wakeup_t **out_wakeup)
{
    if (!out_wakeup) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zb_wakeup_t *wakeup = malloc(sizeof(*wakeup));
    if (!wakeup) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status = zb_mutex_init(&wakeup->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        free(wakeup);
        return status;
    }
    int descriptors[2];
    if (pipe2(descriptors, O_NONBLOCK | O_CLOEXEC) != 0) {
        status = socket_error(errno);
        (void)zb_mutex_destroy(&wakeup->mutex);
        free(wakeup);
        return status;
    }
    wakeup->reader = descriptors[0];
    wakeup->writer = descriptors[1];
    wakeup->pending = false;
    *out_wakeup = wakeup;
    return ZEROBUS_STATUS_OK;
}

void zb_wakeup_free(zb_wakeup_t *wakeup)
{
    if (!wakeup) {
        return;
    }
    close(wakeup->writer);
    close(wakeup->reader);
    (void)zb_mutex_destroy(&wakeup->mutex);
    free(wakeup);
}

zerobus_status_t zb_wakeup_signal(zb_wakeup_t *wakeup)
{
    if (!wakeup) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zerobus_status_t status = zb_mutex_lock(&wakeup->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (!wakeup->pending) {
        const uint8_t byte = 1;
        ssize_t result;
        do {
            result = write(wakeup->writer, &byte, 1);
        } while (result < 0 && errno == EINTR);
        if (result == 1 ||
            (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))) {
            wakeup->pending = true;
        } else {
            status = ZEROBUS_STATUS_INTERNAL;
        }
    }
    (void)zb_mutex_unlock(&wakeup->mutex);
    return status;
}

zerobus_status_t zb_wakeup_drain(zb_wakeup_t *wakeup)
{
    if (!wakeup) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zerobus_status_t status = zb_mutex_lock(&wakeup->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    uint8_t bytes[256];
    for (;;) {
        ssize_t result = read(wakeup->reader, bytes, sizeof(bytes));
        if (result > 0 || (result < 0 && errno == EINTR)) {
            continue;
        }
        if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            wakeup->pending = false;
        } else {
            status = ZEROBUS_STATUS_INTERNAL;
        }
        break;
    }
    (void)zb_mutex_unlock(&wakeup->mutex);
    return status;
}

bool zb_wakeup_pending(zb_wakeup_t *wakeup)
{
    if (!wakeup || zb_mutex_lock(&wakeup->mutex) != ZEROBUS_STATUS_OK) {
        return false;
    }
    bool pending = wakeup->pending;
    (void)zb_mutex_unlock(&wakeup->mutex);
    return pending;
}

static int poll_timeout(zb_deadline_t deadline)
{
    if (deadline == ZB_DEADLINE_INFINITE) {
        return -1;
    }
    uint64_t millis = zb_deadline_remaining_ms(deadline);
    return millis > INT_MAX ? INT_MAX : (int)millis;
}

zerobus_status_t zb_net_wait(const zb_net_interest_t *interest,
                             zb_wakeup_t *wakeup, unsigned *out_events)
{
    if (!interest || !wakeup || !out_events) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    struct pollfd descriptors[2] = {
        {.fd = wakeup->reader, .events = POLLIN},
        {.fd = -1},
    };
    if (interest->socket != ZB_SOCKET_INVALID && interest->events) {
        descriptors[1].fd = interest->socket;
        if (interest->events & ZB_NET_READ) {
            descriptors[1].events |= POLLIN;
        }
        if (interest->events & ZB_NET_WRITE) {
            descriptors[1].events |= POLLOUT;
        }
    }
    int result;
    for (;;) {
        result = poll(descriptors, 2, poll_timeout(interest->deadline));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result == 0 && !zb_deadline_expired(interest->deadline)) {
            continue;
        }
        break;
    }
    if (result < 0) {
        return socket_error(errno);
    }
    if (descriptors[0].revents & (POLLERR | POLLHUP | POLLNVAL)) {
        return ZEROBUS_STATUS_INTERNAL;
    }
    unsigned events = 0;
    if (descriptors[1].revents & POLLIN) {
        events |= ZB_NET_READ;
    }
    if (descriptors[1].revents & POLLOUT) {
        events |= ZB_NET_WRITE;
    }
    if (descriptors[1].revents & (POLLERR | POLLHUP | POLLNVAL)) {
        events |= ZB_NET_ERROR;
    }
    *out_events = events;
    return ZEROBUS_STATUS_OK;
}
