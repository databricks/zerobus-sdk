#include <stdlib.h>
#include <string.h>

#include "io.h"

static void close_network(zb_io_t *io)
{
    zb_tls_free(io->tls);
    io->tls = NULL;
    io->tls_context = NULL;
    free(io->tls_host);
    io->tls_host = NULL;
    zb_socket_close(io->socket);
    io->socket = ZB_SOCKET_INVALID;
    zb_net_addresses_free(io->addresses);
    io->addresses = NULL;
}

zerobus_status_t zb_io_init(zb_io_t *io, const zb_grpc_config_t *config,
                            const char *host, uint16_t port,
                            const zb_tls_ctx_t *tls_context,
                            zb_deadline_t deadline)
{
    *io = (zb_io_t){.socket = ZB_SOCKET_INVALID, .deadline = deadline};
    if (!config || config->tls != (tls_context != NULL)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zerobus_status_t status = zb_wakeup_new(&io->wakeup);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    io->tls_context = tls_context;
    status = zb_grpc_init(&io->grpc, config);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }
    if (zb_deadline_expired(deadline)) {
        status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        goto fail;
    }
    status = zb_net_resolve(host, port, &io->addresses);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }
    if (zb_deadline_expired(deadline)) {
        status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        goto fail;
    }
    if (config->tls) {
        size_t host_len = strlen(host);
        io->tls_host = malloc(host_len + 1);
        if (!io->tls_host) {
            status = ZEROBUS_STATUS_OUT_OF_MEMORY;
            goto fail;
        }
        memcpy(io->tls_host, host, host_len + 1);
    }
    if (zb_deadline_expired(deadline)) {
        status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        goto fail;
    }
    return ZEROBUS_STATUS_OK;
fail:
    zb_io_destroy(io);
    return status;
}

void zb_io_destroy(zb_io_t *io)
{
    zb_grpc_destroy(&io->grpc);
    close_network(io);
    zb_wakeup_free(io->wakeup);
    io->wakeup = NULL;
}

static bool setup_expired(const zb_io_t *io)
{
    return zb_deadline_expired(io->deadline);
}

static zerobus_status_t wait_setup(zb_io_t *io, unsigned want)
{
    for (;;) {
        zerobus_status_t status = zb_wakeup_drain(io->wakeup);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        status = io->service(io->context);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        if (setup_expired(io)) {
            return ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        }
        zb_net_interest_t interest = {io->socket, want, io->deadline};
        unsigned events = 0;
        status = zb_net_wait(&interest, io->wakeup, &events);
        if (status != ZEROBUS_STATUS_OK || (events & (want | ZB_NET_ERROR))) {
            return status;
        }
    }
}

static zerobus_status_t connect_socket(zb_io_t *io)
{
    bool connected = false;
    while (!connected) {
        zerobus_status_t status = io->service(io->context);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        if (setup_expired(io)) {
            return ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        }
        status = zb_socket_connect_next(io->addresses, &io->socket, &connected);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        if (!connected) {
            status = wait_setup(io, ZB_NET_WRITE);
            if (status != ZEROBUS_STATUS_OK) {
                return status;
            }
            connected =
                zb_socket_finish_connect(io->socket) == ZEROBUS_STATUS_OK;
            if (!connected) {
                zb_socket_close(io->socket);
                io->socket = ZB_SOCKET_INVALID;
            }
        }
    }
    zb_net_addresses_free(io->addresses);
    io->addresses = NULL;
    if (io->tls_context) {
        zerobus_status_t status = zb_tls_new(io->tls_context, io->socket,
                                             io->tls_host, true, &io->tls);
        io->tls_context = NULL;
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        bool ready = false;
        while (!ready) {
            status = io->service(io->context);
            if (status != ZEROBUS_STATUS_OK) {
                return status;
            }
            unsigned want = 0;
            status = zb_tls_handshake(io->tls, &want, &ready);
            if (status != ZEROBUS_STATUS_OK) {
                return status;
            }
            if (!ready) {
                status = wait_setup(io, want);
                if (status != ZEROBUS_STATUS_OK) {
                    return status;
                }
            }
        }
    }
    return setup_expired(io) ? ZEROBUS_STATUS_DEADLINE_EXCEEDED
                             : ZEROBUS_STATUS_OK;
}

static zerobus_status_t protocol_status(const zb_grpc_t *grpc, ssize_t result)
{
    if (grpc->failure != ZEROBUS_STATUS_OK || result >= 0) {
        return grpc->failure;
    }
    return zb_map_nghttp2_error((int)result);
}

/* Copies frames out of nghttp2 into io->output until it is full or nghttp2
 * has nothing more to send, which sets *dry. Unwritten bytes move to the
 * front first. A frame that does not fit continues on the next call from
 * *frame, which stays valid until the next nghttp2_session_mem_send. Must not
 * run while a write waits to be retried with the same bytes. */
static zerobus_status_t fill_output(zb_io_t *io, const uint8_t **frame,
                                    size_t *frame_len, bool *dry)
{
    size_t used = io->output_end - io->output_start;
    if (io->output_start != 0) {
        memmove(io->output, io->output + io->output_start, used);
        io->output_start = 0;
        io->output_end = used;
    }
    *dry = false;
    while (io->output_end < sizeof(io->output)) {
        if (*frame_len == 0) {
            ssize_t len = nghttp2_session_mem_send(io->grpc.session, frame);
            zerobus_status_t status = protocol_status(&io->grpc, len);
            if (status != ZEROBUS_STATUS_OK) {
                return status;
            }
            if (len == 0) {
                *dry = true;
                break;
            }
            *frame_len = (size_t)len;
        }
        size_t room = sizeof(io->output) - io->output_end;
        size_t take = *frame_len < room ? *frame_len : room;
        memcpy(io->output + io->output_end, *frame, take);
        io->output_end += take;
        *frame += take;
        *frame_len -= take;
    }
    return ZEROBUS_STATUS_OK;
}

void *zb_io_run(void *arg)
{
    zb_io_t *io = arg;
    zb_grpc_t *grpc = &io->grpc;
    zerobus_status_t status = connect_socket(io);
    if (status != ZEROBUS_STATUS_OK) {
        goto done;
    }
    /* Publish TCP/TLS readiness before any HTTP/2 response callbacks. */
    io->on_ready(io->context);

    unsigned events = 0, read_want = 0, write_want = 0;
    const uint8_t *input = io->input, *frame = NULL;
    size_t input_len = 0, frame_len = 0, write_len = 0;
    bool dry = true;
    io->output_start = 0;
    io->output_end = 0;
    for (;;) {
        /* Drain before observing control/channels, so concurrent publication
         * is observed here or leaves a fresh pipe notification. */
        status = zb_wakeup_drain(io->wakeup);
        if (status != ZEROBUS_STATUS_OK) {
            break;
        }
        status = io->service(io->context);
        if (status != ZEROBUS_STATUS_OK) {
            break;
        }
        bool was_paused = grpc->paused;
        if (was_paused) {
            status = zb_grpc_retry_receive(grpc);
            if (status != ZEROBUS_STATUS_OK) {
                break;
            }
        }
        if (!grpc->paused && (was_paused || input_len != 0)) {
            /* Even an empty suffix must finish the paused DATA frame before
             * the input buffer can be overwritten. */
            ssize_t consumed =
                nghttp2_session_mem_recv(grpc->session, input, input_len);
            status = protocol_status(grpc, consumed);
            if (status != ZEROBUS_STATUS_OK) {
                break;
            }
            input += (size_t)consumed;
            input_len -= (size_t)consumed;
        }
        if (events & ZB_NET_ERROR) {
            events |= ZB_NET_READ | ZB_NET_WRITE;
        }
        /* Probe for input once per turn even without a readiness event, so
         * window updates and acknowledgments are not left unread while
         * writes keep making progress. */
        if (read_want == ZB_NET_READ) {
            read_want = 0;
        }
        bool progress = false;
        /* Bound each turn so cancellation and newly published work are
         * observed during sustained socket traffic, without sleeping. */
        for (unsigned step = 0; step < 4; ++step) {
            progress = false;
            bool can_read = !grpc->paused && input_len == 0 &&
                            nghttp2_session_want_read(grpc->session);
            bool buffered = io->tls && zb_tls_pending(io->tls);
            /* Cross-direction WANT is an unfinished TLS operation and takes
             * priority over starting the opposite operation. read_want == 0
             * also probes records buffered by handshake or a completed write.
             */
            if (can_read && write_want != ZB_NET_READ &&
                (buffered || !read_want || (events & read_want))) {
                zb_io_result_t result;
                status = io->tls ? zb_tls_read(io->tls, io->input,
                                               sizeof(io->input), &result)
                                 : zb_socket_read(io->socket, io->input,
                                                  sizeof(io->input), &result);
                if (status != ZEROBUS_STATUS_OK) {
                    goto done;
                }
                if (result.eof) {
                    status = ZEROBUS_STATUS_UNAVAILABLE;
                    goto done;
                }
                read_want = result.want;
                events &= ~result.want;
                if (!result.want) {
                    progress = result.bytes != 0;
                    input = io->input;
                    input_len = result.bytes;
                    ssize_t consumed = nghttp2_session_mem_recv(
                        grpc->session, input, input_len);
                    status = protocol_status(grpc, consumed);
                    if (status != ZEROBUS_STATUS_OK) {
                        goto done;
                    }
                    input += (size_t)consumed;
                    input_len -= (size_t)consumed;
                }
            }
            if (read_want != ZB_NET_WRITE &&
                (!write_want || (events & write_want))) {
                if (!write_want) {
                    /* A retry needs identical arguments; otherwise refill
                     * and choose what to write. While more output follows,
                     * TLS writes whole records and keeps the tail. */
                    status = fill_output(io, &frame, &frame_len, &dry);
                    if (status != ZEROBUS_STATUS_OK) {
                        goto done;
                    }
                    write_len = io->output_end - io->output_start;
                    if (io->tls && !dry && write_len > ZB_IO_CHUNK) {
                        write_len -= write_len % ZB_IO_CHUNK;
                    }
                }
                if (write_len) {
                    const uint8_t *output = io->output + io->output_start;
                    zb_io_result_t result;
                    status = io->tls ? zb_tls_write(io->tls, output, write_len,
                                                    &result)
                                     : zb_socket_write(io->socket, output,
                                                       write_len, &result);
                    if (status != ZEROBUS_STATUS_OK) {
                        goto done;
                    }
                    if (result.eof) {
                        status = ZEROBUS_STATUS_UNAVAILABLE;
                        goto done;
                    }
                    if (!result.want) {
                        if (write_want == ZB_NET_READ) {
                            read_want = 0;
                        }
                        io->output_start += result.bytes;
                        if (io->output_start == io->output_end) {
                            io->output_start = 0;
                            io->output_end = 0;
                        }
                        progress |= result.bytes != 0;
                    }
                    write_want = result.want;
                    events &= ~result.want;
                }
            }
            if (!progress) {
                break;
            }
        }
        events = 0;
        if (progress) {
            continue;
        }
        unsigned want = write_want;
        if (!grpc->paused && input_len == 0 &&
            nghttp2_session_want_read(grpc->session)) {
            want |= read_want ? read_want : ZB_NET_READ;
        }
        /* Messages queued while output waits for the socket are not work
         * yet: skipping the wait would never observe writability. */
        if (io->prepare_wait(io->context,
                             write_want == 0 && read_want != ZB_NET_WRITE)) {
            continue;
        }
        zb_net_interest_t interest = {io->socket, want, ZB_DEADLINE_INFINITE};
        status = zb_net_wait(&interest, io->wakeup, &events);
        if (status != ZEROBUS_STATUS_OK) {
            break;
        }
    }
done:
    if (status == ZEROBUS_STATUS_OK) {
        status = ZEROBUS_STATUS_CANCELLED;
    }
    zb_grpc_abort(grpc, status);
    zb_grpc_destroy(grpc);
    close_network(io);
    io->on_stopped(io->context, status);
    return NULL;
}
