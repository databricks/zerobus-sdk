/* A bounded FIFO of messages from one producer to one consumer, with blocking
 * and nonblocking access. */
#ifndef ZB_NETWORK_CHANNEL_H
#define ZB_NETWORK_CHANNEL_H

#include "internal/platform/network.h"
#include "message.h"

typedef struct zb_channel zb_channel_t;

/* Creates a channel that holds up to max_messages messages, which must be at
 * least 1 and less than ZB_SEM_VALUE_MAX. push_wakeup and pop_wakeup may be
 * NULL. Otherwise each is signalled after every push or pop, and both on
 * close. They must outlive the channel. */
zerobus_status_t zb_channel_new(size_t max_messages, zb_wakeup_t *push_wakeup,
                                zb_wakeup_t *pop_wakeup,
                                zb_channel_t **out_channel);

/* Frees the channel and the messages still queued, once all its users have
 * stopped. Accepts NULL. */
void zb_channel_free(zb_channel_t *channel);

/* Queues message, waiting until deadline for room. On success the channel
 * owns message and its data, on failure the caller keeps them.
 * FAILED_PRECONDITION once the channel is closed. */
zerobus_status_t zb_channel_push(zb_channel_t *channel, zb_message_t *message,
                                 zb_deadline_t deadline);

/* Like push, but RESOURCE_EXHAUSTED instead of waiting for room. */
zerobus_status_t zb_channel_try_push(zb_channel_t *channel,
                                     zb_message_t *message);

/* Hands the oldest message and its data to the caller, waiting until deadline
 * for one. Once the channel is closed, returns the messages left, then OK with
 * *out_message NULL. Failures leave *out_message unchanged. */
zerobus_status_t zb_channel_pop(zb_channel_t *channel, zb_deadline_t deadline,
                                zb_message_t **out_message);

/* Like pop, but RESOURCE_EXHAUSTED instead of waiting for a message. */
zerobus_status_t zb_channel_try_pop(zb_channel_t *channel,
                                    zb_message_t **out_message);

/* Refuses further pushes, wakes a waiting producer or consumer, and signals
 * both wakeups. Returns the first signal failure, but closes the channel
 * either way. A push that overlaps close either fails, or succeeds and its
 * message is popped before the channel's end. */
zerobus_status_t zb_channel_close(zb_channel_t *channel);

#endif /* ZB_NETWORK_CHANNEL_H */
