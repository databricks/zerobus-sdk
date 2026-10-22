/* One producer, one consumer; blocking worker and non-waiting I/O access. */
#ifndef ZB_NETWORK_CHANNEL_H
#define ZB_NETWORK_CHANNEL_H

#include "internal/platform/network.h"
#include "message.h"

typedef struct zb_channel zb_channel_t;

/* Fixed slot count; max_messages need not be a power of two. No byte budget.
 * wakeup is optional and borrowed until free. */
zerobus_status_t zb_channel_new(size_t max_messages, zb_wakeup_t *wakeup,
                                zb_channel_t **out_channel);
/* Free only after all users have stopped. Frees all queued messages, but not
 * messages already popped and owned by a consumer. */
void zb_channel_free(zb_channel_t *channel);

/* Success transfers descriptor and payload ownership. Failure leaves both
 * with the caller. push waits for space until deadline and wakes I/O if it
 * armed a wakeup; try_push never waits for capacity and does not wake I/O.
 * Notification follows committed ownership transfers and cannot undo them. */
zerobus_status_t zb_channel_push(zb_channel_t *channel, zb_message_t *message,
                                 zb_deadline_t deadline);
zerobus_status_t zb_channel_try_push(zb_channel_t *channel,
                                     zb_message_t *message);

/* Each instance has one consumer. Pop transfers descriptor/payload ownership
 * and immediately returns the queue slot. pop waits, and wakes I/O as space
 * returns if it armed a wakeup; try_pop neither waits for an item nor wakes
 * I/O. OK with *out_message == NULL means closed with no queued item. When
 * close follows the producer's last push, this is stable EOF, including repeat
 * calls. try_pop returns RESOURCE_EXHAUSTED when open and empty. Failures
 * leave out_message unchanged. */
zerobus_status_t zb_channel_pop(zb_channel_t *channel, zb_deadline_t deadline,
                                zb_message_t **out_message);

zerobus_status_t zb_channel_try_pop(zb_channel_t *channel,
                                    zb_message_t **out_message);

/* Called by I/O, as the consumer, before it sleeps. Asks the producer to
 * signal the wakeup after its next push. Returns true when I/O already has
 * work instead: a queued message, or the end of a closed channel. */
bool zb_channel_arm_pop_wakeup(zb_channel_t *channel);

/* Called by I/O, as the producer, before it sleeps holding a refused message.
 * Asks the consumer to signal the wakeup after its next pop. Returns true when
 * space, or closure, is already there to retry the push. */
bool zb_channel_arm_push_wakeup(zb_channel_t *channel);

/* Reject new pushes and wake the producer, consumer, and I/O. For graceful
 * draining, call after the producer's last push completes. Cancellation may
 * race a push to unblock it; an in-flight push may still transfer ownership
 * after close returns, so that overlap does not guarantee stable EOF.
 * The channel closes even if signalling I/O fails; a repeat retries the wake.
 */
zerobus_status_t zb_channel_close(zb_channel_t *channel);

#endif /* ZB_NETWORK_CHANNEL_H */
