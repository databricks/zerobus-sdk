#ifndef ZB_EPHEMERAL_STREAM_SUPERVISOR_H
#define ZB_EPHEMERAL_STREAM_SUPERVISOR_H

/* Supervisor thread for one stream (a zb_ephemeral_stream_t *). Opens calls:
 * resolves headers, connects, and runs the create-stream handshake. Runs
 * each call's sender and receiver, recovers from retryable failures by
 * replaying unacknowledged records on a new call, and fails the stream
 * otherwise. Publishes the first open's result and exits when the stream
 * finishes, aborts or fails. */
void *zb_supervisor_main(void *stream);

#endif /* ZB_EPHEMERAL_STREAM_SUPERVISOR_H */
