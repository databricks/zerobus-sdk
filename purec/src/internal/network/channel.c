#define _POSIX_C_SOURCE 200809L

#include <stdatomic.h>
#include <stdlib.h>

#include "channel.h"
#include "internal/common/queue.h"

struct zb_channel {
    zb_sem_t space;
    zb_sem_t items;
    zb_queue_t *queue;
    zb_wakeup_t *push_wakeup;
    zb_wakeup_t *pop_wakeup;
    /* Set under mutex, which a push holds from its last check of closed until
     * its message is queued. */
    atomic_bool closed;
    zb_mutex_t mutex;
};

/* Signals wakeup, unless it is NULL. */
static zerobus_status_t notify(zb_wakeup_t *wakeup)
{
    return wakeup ? zb_wakeup_signal(wakeup) : ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_channel_new(size_t max_messages, zb_wakeup_t *push_wakeup,
                                zb_wakeup_t *pop_wakeup,
                                zb_channel_t **out_channel)
{
    /* Both semaphores need room for one extra close permit. */
    if (!out_channel || !max_messages || max_messages >= ZB_SEM_VALUE_MAX) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    size_t capacity = 4;
    while (capacity < max_messages) {
        if (capacity > SIZE_MAX / 2) {
            return ZEROBUS_STATUS_INVALID_ARGUMENT;
        }
        capacity <<= 1;
    }
    zb_channel_t *channel = calloc(1, sizeof(*channel));
    if (!channel) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status = zb_queue_new(capacity, &channel->queue);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }
    atomic_init(&channel->closed, false);
    status = zb_sem_init(&channel->space, (unsigned int)max_messages);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail_queue;
    }
    status = zb_sem_init(&channel->items, 0);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail_space;
    }
    status = zb_mutex_init(&channel->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail_items;
    }
    channel->push_wakeup = push_wakeup;
    channel->pop_wakeup = pop_wakeup;
    *out_channel = channel;
    return ZEROBUS_STATUS_OK;
fail_items:
    (void)zb_sem_destroy(&channel->items);
fail_space:
    (void)zb_sem_destroy(&channel->space);
fail_queue:
    zb_queue_free(channel->queue);
fail:
    free(channel);
    return status;
}

void zb_channel_free(zb_channel_t *channel)
{
    if (!channel) {
        return;
    }
    while (zb_queue_advance(channel->queue, NULL)) {
    }
    zb_message_t *message;
    while ((message = zb_queue_pop(channel->queue, NULL))) {
        free(message->data);
        free(message);
    }
    zb_queue_free(channel->queue);
    (void)zb_mutex_destroy(&channel->mutex);
    (void)zb_sem_destroy(&channel->items);
    (void)zb_sem_destroy(&channel->space);
    free(channel);
}

static zerobus_status_t push(zb_channel_t *channel, zb_message_t *message,
                             zb_deadline_t deadline, bool blocking)
{
    if (!channel || !message || (!message->data && message->len)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (atomic_load(&channel->closed)) {
        return ZEROBUS_STATUS_FAILED_PRECONDITION;
    }
    zerobus_status_t status = zb_sem_wait(&channel->space, deadline);
    if (status != ZEROBUS_STATUS_OK) {
        return !blocking && status == ZEROBUS_STATUS_DEADLINE_EXCEEDED
                   ? ZEROBUS_STATUS_RESOURCE_EXHAUSTED
                   : status;
    }
    /* Under mutex, close either waits until the message is queued, so pop
     * returns it before the end, or comes first and refuses it. */
    (void)zb_mutex_lock(&channel->mutex);
    if (atomic_load(&channel->closed)) {
        (void)zb_mutex_unlock(&channel->mutex);
        (void)zb_sem_post(&channel->space);
        return ZEROBUS_STATUS_FAILED_PRECONDITION;
    }
    status = zb_queue_push(channel->queue, message, NULL);
    (void)zb_mutex_unlock(&channel->mutex);
    if (status == ZEROBUS_STATUS_OK) {
        (void)zb_sem_post(&channel->items);
    } else {
        (void)zb_sem_post(&channel->space);
    }
    /* The message is queued either way, so a failed signal is not reported. */
    if (status == ZEROBUS_STATUS_OK) {
        (void)notify(channel->push_wakeup);
    }
    return status;
}

zerobus_status_t zb_channel_push(zb_channel_t *channel, zb_message_t *message,
                                 zb_deadline_t deadline)
{
    return push(channel, message, deadline, true);
}

zerobus_status_t zb_channel_try_push(zb_channel_t *channel,
                                     zb_message_t *message)
{
    return push(channel, message, ZB_DEADLINE_IMMEDIATE, false);
}

static zerobus_status_t pop(zb_channel_t *channel, zb_deadline_t deadline,
                            bool blocking, zb_message_t **out_message)
{
    if (!channel || !out_message) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    /* Once closed, pop takes what is left without waiting. */
    if (!atomic_load(&channel->closed)) {
        zerobus_status_t status = zb_sem_wait(&channel->items, deadline);
        if (status != ZEROBUS_STATUS_OK) {
            return !blocking && status == ZEROBUS_STATUS_DEADLINE_EXCEEDED
                       ? ZEROBUS_STATUS_RESOURCE_EXHAUSTED
                       : status;
        }
    }
    /* No message here means the channel is closed and empty. */
    zb_message_t *message = NULL;
    if (zb_queue_advance(channel->queue, NULL)) {
        message = zb_queue_pop(channel->queue, NULL);
        (void)zb_sem_post(&channel->space);
        (void)notify(channel->pop_wakeup);
    }
    *out_message = message;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_channel_pop(zb_channel_t *channel, zb_deadline_t deadline,
                                zb_message_t **out_message)
{
    return pop(channel, deadline, true, out_message);
}

zerobus_status_t zb_channel_try_pop(zb_channel_t *channel,
                                    zb_message_t **out_message)
{
    return pop(channel, ZB_DEADLINE_IMMEDIATE, false, out_message);
}

zerobus_status_t zb_channel_close(zb_channel_t *channel)
{
    if (!channel) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    (void)zb_mutex_lock(&channel->mutex);
    bool was_closed = atomic_exchange(&channel->closed, true);
    (void)zb_mutex_unlock(&channel->mutex);
    if (!was_closed) {
        (void)zb_sem_post(&channel->space);
        (void)zb_sem_post(&channel->items);
    }
    zerobus_status_t status = notify(channel->push_wakeup);
    zerobus_status_t pop_status = notify(channel->pop_wakeup);
    return status == ZEROBUS_STATUS_OK ? pop_status : status;
}
