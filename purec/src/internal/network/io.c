#include <stdlib.h>
#include <string.h>

#include "internal/common/utils.h"
#include "io.h"

/* ---- lifecycle --------------------------------------------------------- */

/* Closes the connection and frees the host copy. Safe to call again. */
static void close_network(zb_io_t *io)
{
    zb_connection_close(&io->connection);
    io->tls_context = NULL;
    free(io->host);
    io->host = NULL;
}

zerobus_status_t zb_io_init(zb_io_t *io, const zb_grpc_config_t *config,
                            zerobus_string_view_t host, uint16_t port,
                            const zb_tls_ctx_t *tls_context,
                            zb_deadline_t connect_deadline)
{
    *io = (zb_io_t){.connection = ZB_CONNECTION_CLOSED,
                    .port = port,
                    .connect_deadline = connect_deadline};
    if (!config || !port || config->tls != (tls_context != NULL) ||
        !host.data || !host.len || host.len == SIZE_MAX ||
        memchr(host.data, '\0', host.len)) {
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
    io->host = zb_strdup_view(host);
    if (!io->host) {
        status = ZEROBUS_STATUS_OUT_OF_MEMORY;
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

/* ---- I/O loop ---------------------------------------------------------- */

/* Read and write steps per iteration, so service still runs under steady
 * traffic. */
#define STEPS_PER_ITERATION 4u

static zerobus_status_t protocol_status(const zb_grpc_t *grpc, ssize_t result)
{
    if (grpc->failure != ZEROBUS_STATUS_OK || result >= 0) {
        return grpc->failure;
    }
    return zb_map_nghttp2_error((int)result);
}

typedef struct {
    /* Received bytes gRPC has not consumed yet, in io->input. */
    const uint8_t *input;
    size_t input_len;
    /* Output from nghttp2 not yet written. */
    const uint8_t *output;
    size_t output_len;
    zerobus_status_t output_status; /* Reported once output is written. */

    /* Socket events from the last wait. */
    unsigned events;
    /* What the last read and the last write wait for. */
    unsigned read_want;
    unsigned write_want;
    /* Whether the last step read or wrote bytes. */
    bool progress;
} loop_state_t;

/* Whether gRPC takes input now: it has not failed or paused, has consumed all
 * input so far, and the session expects more. */
static bool wants_input(const zb_io_t *io, const loop_state_t *state)
{
    return io->grpc.failure == ZEROBUS_STATUS_OK && !io->grpc.paused &&
           state->input_len == 0 && nghttp2_session_want_read(io->grpc.session);
}

static zerobus_status_t feed_input(zb_io_t *io, loop_state_t *state)
{
    ssize_t consumed = nghttp2_session_mem_recv(io->grpc.session, state->input,
                                                state->input_len);
    zerobus_status_t status = protocol_status(&io->grpc, consumed);
    if (status == ZEROBUS_STATUS_OK) {
        state->input += (size_t)consumed;
        state->input_len -= (size_t)consumed;
    }
    return status;
}

static zerobus_status_t retry_paused(zb_io_t *io, loop_state_t *state)
{
    bool was_paused = io->grpc.paused;
    if (was_paused) {
        zerobus_status_t status = zb_grpc_retry_receive(&io->grpc);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
    }
    if (!io->grpc.paused && (was_paused || state->input_len != 0)) {
        /* The rest of the input, even if empty, finishes the paused frame. */
        return feed_input(io, state);
    }
    return ZEROBUS_STATUS_OK;
}

/* Reads a chunk from the connection and feeds it to gRPC, if gRPC takes input
 * and nothing holds the read back. Sets progress if it read bytes. Returns
 * UNAVAILABLE at EOF, or the first failure. */
static zerobus_status_t try_read(zb_io_t *io, loop_state_t *state)
{
    if (!wants_input(io, state)) {
        return ZEROBUS_STATUS_OK;
    }
    /* A write that TLS left waiting for READ reads the socket itself, so it
     * goes first. */
    if (state->write_want == ZB_NET_READ) {
        return ZEROBUS_STATUS_OK;
    }
    bool pending = zb_connection_pending(&io->connection);
    /* A read that had to wait goes again once its event arrives, or once TLS
     * holds decrypted data. Any other read goes right away. */
    if (state->read_want && !(state->events & state->read_want) && !pending) {
        return ZEROBUS_STATUS_OK;
    }
    zb_io_result_t result;
    zerobus_status_t status = zb_connection_read(&io->connection, io->input,
                                                 sizeof(io->input), &result);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (result.eof) {
        return ZEROBUS_STATUS_UNAVAILABLE;
    }
    state->read_want = result.want;
    state->events &= ~result.want;
    if (!result.want) {
        state->progress |= result.bytes != 0;
        state->input = io->input;
        state->input_len = result.bytes;
        return feed_input(io, state);
    }
    return ZEROBUS_STATUS_OK;
}

/* Writes up to one chunk of gRPC's output, taking more from nghttp2 once the
 * last is written. Writes only when no read waits for WRITE, and the write
 * does not wait or its event has arrived. Sets progress if it wrote bytes.
 * Returns the first failure. A gRPC failure that comes with output is returned
 * once that output is written. */
static zerobus_status_t try_write(zb_io_t *io, loop_state_t *state)
{
    /* Once gRPC fails, stop writing, unless the failure came with the output
     * being written, such as an error GOAWAY. */
    if (io->grpc.failure != ZEROBUS_STATUS_OK &&
        state->output_status == ZEROBUS_STATUS_OK) {
        return io->grpc.failure;
    }
    if (state->read_want == ZB_NET_WRITE ||
        (state->write_want && !(state->events & state->write_want))) {
        return ZEROBUS_STATUS_OK;
    }
    if (!state->output_len) {
        ssize_t len =
            nghttp2_session_mem_send(io->grpc.session, &state->output);
        if (len <= 0) {
            return protocol_status(&io->grpc, len);
        }
        /* An error GOAWAY aborts the session as it is sent: write it out,
         * then report the failure. */
        state->output_len = (size_t)len;
        state->output_status = io->grpc.failure;
    }
    /* After a blocked write, TLS needs the same data and len again. */
    size_t len =
        state->output_len < ZB_IO_CHUNK ? state->output_len : ZB_IO_CHUNK;
    zb_io_result_t result;
    zerobus_status_t status =
        zb_connection_write(&io->connection, state->output, len, &result);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (!result.want) {
        /* TLS read the socket for this write and may have buffered records,
         * so the next read goes without waiting for the socket. */
        if (state->write_want == ZB_NET_READ) {
            state->read_want = 0;
        }
        state->output += result.bytes;
        state->output_len -= result.bytes;
        state->progress |= result.bytes != 0;
    }
    state->write_want = result.want;
    state->events &= ~result.want;
    return state->output_len ? ZEROBUS_STATUS_OK : state->output_status;
}

void *zb_io_run(void *arg)
{
    zb_io_t *io = arg;
    zb_grpc_t *grpc = &io->grpc;
    zerobus_status_t status =
        zb_connection_open(&io->connection, io->tls_context, io->host, io->port,
                           true, io->wakeup, io->connect_deadline);
    io->tls_context = NULL;
    if (status != ZEROBUS_STATUS_OK) {
        goto done;
    }
    io->on_ready(io->context);
    loop_state_t state = {.input = io->input};
    for (;;) {
        status = zb_wakeup_drain(io->wakeup);
        if (status != ZEROBUS_STATUS_OK) {
            break;
        }
        status = io->service(io->context);
        if (status != ZEROBUS_STATUS_OK) {
            break;
        }
        if (grpc->failure == ZEROBUS_STATUS_OK) {
            status = retry_paused(io, &state);
            if (status != ZEROBUS_STATUS_OK) {
                break;
            }
        }
        /* A socket error or hang-up counts as ready for both directions, so a
         * waiting read or write retries and reports it. */
        if (state.events & ZB_NET_ERROR) {
            state.events |= ZB_NET_READ | ZB_NET_WRITE;
        }
        for (unsigned step = 0; step < STEPS_PER_ITERATION; ++step) {
            state.progress = false;
            status = try_read(io, &state);
            if (status != ZEROBUS_STATUS_OK) {
                goto done;
            }
            status = try_write(io, &state);
            if (status != ZEROBUS_STATUS_OK) {
                goto done;
            }
            if (!state.progress) {
                break;
            }
        }
        /* After a GOAWAY, once its last call has ended and all output is
         * written, the session wants neither direction: the connection is
         * over. */
        if (state.output_len == 0 &&
            !nghttp2_session_want_read(grpc->session) &&
            !nghttp2_session_want_write(grpc->session)) {
            status = ZEROBUS_STATUS_UNAVAILABLE;
            break;
        }
        /* Wait for what the write waits for and, if gRPC takes input, for what
         * the read waits for, or READ when the read is not waiting. After
         * progress, only take the events ready now, so a waiting read or write
         * still sees them while the other keeps going. */
        unsigned want = state.write_want;
        if (wants_input(io, &state)) {
            want |= state.read_want ? state.read_want : ZB_NET_READ;
        }
        zb_deadline_t deadline =
            state.progress ? ZB_DEADLINE_IMMEDIATE : ZB_DEADLINE_INFINITE;
        zb_net_interest_t interest = {io->connection.socket, want, deadline};
        status = zb_net_wait(&interest, io->wakeup, &state.events);
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
