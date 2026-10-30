#define _GNU_SOURCE

#include "http_server.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "loopback.h"

static ssize_t server_read(int socket, SSL *ssl, char *data, size_t len)
{
    return ssl ? SSL_read(ssl, data, (int)len) : read(socket, data, len);
}

/* Whether the request head and its Content-Length body have arrived. */
static bool request_complete(const http_server_t *server)
{
    const char *head_end = strstr(server->request, "\r\n\r\n");
    const char *length = strstr(server->request, "Content-Length: ");
    return head_end && length &&
           server->request_len >= (size_t)(head_end + 4 - server->request) +
                                      strtoul(length + 16, NULL, 10);
}

static void serve_one(http_server_t *server, int socket)
{
    SSL *ssl = NULL;
    int enabled = 1;
    server->request_len = 0;
    server->request[0] = '\0';
    if (setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, &enabled,
                   sizeof(enabled)) != 0 ||
        (server->tls &&
         (!(ssl = SSL_new(server->tls)) || SSL_set_fd(ssl, socket) != 1 ||
          SSL_accept(ssl) != 1))) {
        goto done;
    }
    while (!request_complete(server)) {
        size_t room = sizeof(server->request) - 1 - server->request_len;
        ssize_t received =
            room ? server_read(socket, ssl,
                               server->request + server->request_len, room)
                 : 0;
        if (received <= 0) {
            goto done;
        }
        server->request_len += (size_t)received;
        server->request[server->request_len] = '\0';
    }
    for (size_t i = 0; i < HTTP_SERVER_MAX_PIECES && server->reply[i]; ++i) {
        size_t len = strlen(server->reply[i]);
        ssize_t written = ssl ? SSL_write(ssl, server->reply[i], (int)len)
                              : write(socket, server->reply[i], len);
        if (written != (ssize_t)len) {
            goto done;
        }
        usleep(2000);
    }
    char byte;
    while (server->hold && server_read(socket, ssl, &byte, 1) > 0) {
    }
done:
    SSL_free(ssl);
    close(socket);
}

static void *serve(void *arg)
{
    http_server_t *server = arg;
    /* SIGPIPE goes to the thread that writes. Blocked here, a write to a client
     * that has left fails with EPIPE instead of ending the test. */
    sigset_t blocked;
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGPIPE);
    (void)pthread_sigmask(SIG_BLOCK, &blocked, NULL);
    int socket;
    while ((socket = accept(server->listener, NULL, NULL)) >= 0) {
        ++server->served;
        serve_one(server, socket);
    }
    return NULL;
}

bool http_server_start(http_server_t *server)
{
    server->listener = loopback_listen(&server->port);
    if (server->listener < 0) {
        return false;
    }
    if (zb_thread_create(&server->thread, serve, server) != ZEROBUS_STATUS_OK) {
        close(server->listener);
        return false;
    }
    return true;
}

void http_server_stop(http_server_t *server)
{
    /* Wakes the server from accept. */
    shutdown(server->listener, SHUT_RDWR);
    (void)zb_thread_join(&server->thread, NULL);
    close(server->listener);
}
