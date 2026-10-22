#ifndef ZB_EPHEMERAL_STREAM_SENDER_H
#define ZB_EPHEMERAL_STREAM_SENDER_H

/* Sender thread for one attempt (a zb_attempt_t *). Takes records from the
 * landing zone in order, numbers them with the call's physical offsets from
 * 0, and queues them on the call. Once the closed zone is drained, waits for
 * the supervisor's half-close request. Reports through the attempt. */
void *zb_sender_main(void *attempt);

#endif /* ZB_EPHEMERAL_STREAM_SENDER_H */
