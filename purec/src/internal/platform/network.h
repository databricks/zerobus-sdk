/* OS boundary for the nonblocking network driver. */
#ifndef ZB_PLATFORM_NETWORK_H
#define ZB_PLATFORM_NETWORK_H

#include "internal/platform/concurrency.h"

typedef int zb_socket_t;
#define ZB_SOCKET_INVALID (-1)

enum { ZB_NET_READ = 1u, ZB_NET_WRITE = 2u, ZB_NET_ERROR = 4u };

typedef struct {
    zb_socket_t socket;
    unsigned events;
    zb_deadline_t deadline;
} zb_net_interest_t;

typedef struct {
    size_t bytes;
    unsigned want;
    bool eof;
} zb_io_result_t;

typedef struct zb_net_addresses zb_net_addresses_t;
typedef struct zb_wakeup zb_wakeup_t;

/* Resolves host to its TCP addresses for port. Blocks until the lookup
 * completes. */
zerobus_status_t zb_net_resolve(const char *host, uint16_t port,
                                zb_net_addresses_t **out_addresses);
void zb_net_addresses_free(zb_net_addresses_t *addresses);

/* Starts connecting to the next untried address. *out_connected is false while
 * the connection is still in progress. */
zerobus_status_t zb_socket_connect_next(zb_net_addresses_t *addresses,
                                        zb_socket_t *out_socket,
                                        bool *out_connected);

/* Reports whether an in-progress connection succeeded, without blocking. */
zerobus_status_t zb_socket_finish_connect(zb_socket_t socket);
zerobus_status_t zb_socket_read(zb_socket_t socket, uint8_t *data, size_t len,
                                zb_io_result_t *out);
zerobus_status_t zb_socket_write(zb_socket_t socket, const uint8_t *data,
                                 size_t len, zb_io_result_t *out);
void zb_socket_close(zb_socket_t socket);

zerobus_status_t zb_wakeup_new(zb_wakeup_t **out_wakeup);
void zb_wakeup_free(zb_wakeup_t *wakeup);
zerobus_status_t zb_wakeup_signal(zb_wakeup_t *wakeup);
zerobus_status_t zb_wakeup_drain(zb_wakeup_t *wakeup);

/* Sleeps until the socket is ready for one of interest->events, the wakeup is
 * signalled, or the deadline passes. *out_events receives the socket's ready
 * events.
 * ZB_NET_ERROR is included whenever the socket failed or hung up, even if not
 * requested. *out_events is 0 if only a signal or the deadline ended the wait.
 * The signal stays pending until zb_wakeup_drain.
 * If interest->events is 0, the socket is ignored: only a signal or the
 * deadline ends the wait, and a socket failure or hang-up is not reported. */
zerobus_status_t zb_net_wait(const zb_net_interest_t *interest,
                             zb_wakeup_t *wakeup, unsigned *out_events);

#endif /* ZB_PLATFORM_NETWORK_H */
