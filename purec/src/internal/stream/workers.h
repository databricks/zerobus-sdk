/*
 * The threads that carry a stream (src/stream.h) over the transport. Private
 * to the stream.
 *
 * The supervisor, one per stream, runs attempts one after another. An attempt
 * is one call, with a sender that moves records from the landing zone to TX,
 * and a receiver that pops the records RX acknowledges. Physical offsets
 * restart with each call, so a new attempt first replays what the previous
 * one left unacknowledged.
 *
 * Only the supervisor ends an attempt: it sets stop, cancels the call unless
 * it ended cleanly, joins both workers and frees the call. It does so when
 * - close finishes: once everything is acknowledged, the supervisor asks the
 *   sender to half-close, and the server ends the call with OK;
 * - free, or a close whose flush failed, aborts the stream;
 * - a worker fails, or the receiver reads the call's end.
 * After a failure it starts a new attempt or fails the stream. Each worker
 * reports once through the attempt, as its last act, and reports OK when it
 * ends because of stop.
 *
 * The supervisor owns the transport, the call and the attempt, which the
 * workers borrow until joined. The stream owns the landing zone: ingest
 * pushes requests, the sender observes them, the receiver pops and frees the
 * acknowledged ones, and free frees the rest. Neither worker closes a
 * channel: TX ends with the sender's half-close or the call's end, and RX
 * with the call's end.
 */
#ifndef ZB_STREAM_WORKERS_H
#define ZB_STREAM_WORKERS_H

#include <stdatomic.h>

#include "internal/network/transport.h"
#include "stream.h" /* IWYU pragma: keep */

/* One EphemeralStream call and its sender and receiver threads. Owned by the
 * supervisor, which starts and joins both. */
typedef struct {
    zerobus_stream_t *stream;
    zb_transport_call_t *call;
    atomic_bool stop;   /* Supervisor to workers: stop now. */
    atomic_bool paused; /* Server is closing: send no further records. */
    /* Physical offsets handed to the transport; an ack must be below it. */
    _Atomic int64_t sent;
    /* Protected by stream->mutex. */
    bool half_close; /* Supervisor to sender: half-close once drained. */
    bool sender_done;
    zerobus_status_t sender_status;
    bool receiver_done;
    zerobus_status_t receiver_status;
    char *receiver_message;
    bool clean; /* The server ended the call OK after our half-close. */
} zb_attempt_t;

/* Supervisor thread for one stream (a zerobus_stream_t *). Opens calls:
 * resolves headers, connects, and runs the create-stream handshake. Runs
 * each call's sender and receiver, recovers from retryable failures by
 * replaying unacknowledged records on a new call, and fails the stream
 * otherwise. Publishes the first open's result and exits when the stream
 * finishes, aborts or fails. */
void *zb_supervisor_main(void *stream);

/* Sender thread for one attempt (a zb_attempt_t *). Takes records from the
 * landing zone in order, numbers them with the call's physical offsets from
 * 0, and queues them on the call. Once the closed zone is drained, waits for
 * the supervisor's half-close request. Reports through the attempt. */
void *zb_sender_main(void *attempt);

/* Receiver thread for one attempt (a zb_attempt_t *). Applies the server's
 * cumulative acknowledgments: pops each acknowledged record from the landing
 * zone and publishes the acknowledged logical offset. Ends on the call's end,
 * an invalid response, a lack-of-ack timeout, or once a closing server has
 * acknowledged everything sent. Reports through the attempt. */
void *zb_receiver_main(void *attempt);

#endif /* ZB_STREAM_WORKERS_H */
