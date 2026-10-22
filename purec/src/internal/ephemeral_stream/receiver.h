#ifndef ZB_EPHEMERAL_STREAM_RECEIVER_H
#define ZB_EPHEMERAL_STREAM_RECEIVER_H

/* Receiver thread for one attempt (a zb_attempt_t *). Applies the server's
 * cumulative acknowledgments: pops each acknowledged record from the landing
 * zone and publishes the acknowledged logical offset. Ends on the call's end,
 * an invalid response, a lack-of-ack timeout, or once a closing server has
 * acknowledged everything sent. Reports through the attempt. */
void *zb_receiver_main(void *attempt);

#endif /* ZB_EPHEMERAL_STREAM_RECEIVER_H */
