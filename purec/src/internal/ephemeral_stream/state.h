/* State shared by the stream and its supervisor, sender and receiver threads.
 * Private to internal/ephemeral_stream/. */
#ifndef ZB_EPHEMERAL_STREAM_STATE_H
#define ZB_EPHEMERAL_STREAM_STATE_H

#include <stdatomic.h>

#include "ephemeral_stream.h"
#include "internal/common/queue.h"
#include "internal/landing_zone.h"
#include "internal/network/transport.h"

struct zb_ephemeral_stream {
    /* Immutable after new; strings are owned copies. */
    zb_ephemeral_stream_config_t config;
    zb_landing_zone_t *zone;
    /* Acknowledged requests kept for reuse by ingest, so that a burst of
     * acknowledgments does not return memory that ingest at once takes
     * back. Holds at most max_inflight. */
    zb_queue_t *spare;

    /* Highest acknowledged logical offset, -1 before the first. Written by the
     * receiver, which then broadcasts acked_changed under mutex while
     * flush_waiters is nonzero. */
    _Atomic zerobus_offset_t acked;
    atomic_uint flush_waiters;
    zb_cond_t acked_changed;

    /* Everything below is protected by mutex. changed is broadcast on any
     * change here and on each attempt's changes. */
    zb_mutex_t mutex;
    zb_cond_t changed;
    bool opened; /* The first open finished, with open_status. */
    zerobus_status_t open_status;
    zerobus_status_t failure; /* Terminal failure; OK while usable. */
    char *failure_message;
    bool finishing; /* Close flushed: end the call cleanly, then stop. */
    bool aborting;  /* Free, or a close whose flush failed: stop at once. */
    bool closing;
    bool closed;
    zerobus_status_t close_status;
    char *close_message;
    zb_thread_t supervisor;
    bool supervisor_running;
};

/* One EphemeralStream call and its sender and receiver threads. Owned by the
 * supervisor, which starts and joins both. */
typedef struct {
    zb_ephemeral_stream_t *stream;
    zb_transport_call_t *call;
    atomic_bool stop;   /* Supervisor to workers: stop now. */
    atomic_bool paused; /* Server is closing: send no further records. */
    /* Physical offsets handed to the transport; an ack must be below it. */
    _Atomic int64_t sent;
    /* Protected by stream->mutex. */
    bool half_close; /* Supervisor to sender: half-close once drained. */
    bool sender_done;
    bool receiver_done;
    zerobus_status_t sender_status;
    zerobus_status_t receiver_status;
    char *receiver_message;
    bool clean; /* The server ended the call OK after our half-close. */
} zb_attempt_t;

/* Returns an acknowledged request for reuse, or frees it. */
void zb_ephemeral_stream_recycle(zb_ephemeral_stream_t *stream,
                                 zb_ingest_request_t *request);

#endif /* ZB_EPHEMERAL_STREAM_STATE_H */
