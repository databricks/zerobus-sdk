#define _POSIX_C_SOURCE 200809L

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "channel.h"

/* Bits of waiting: who must be notified about the next change. */
enum { WORKER_WAITING = 1u, IO_WAITING = 2u };

/* A ring of message pointers. The producer owns tail and head_seen, the
 * consumer owns head and tail_seen, each on its own cache line. A full or
 * empty side rechecks after publishing its waiting bit, and the other side
 * reads the bits after publishing its index, so either the waiter sees the
 * change or the other side sees the bit. All four use sequential consistency
 * for that reason. The worker side sleeps on the condition variable; I/O
 * never sleeps here and is woken through its wakeup instead. */
struct zb_channel {
    _Alignas(64) atomic_size_t tail;
    size_t head_seen;
    _Alignas(64) atomic_size_t head;
    size_t tail_seen;
    _Alignas(64) atomic_uint waiting;
    atomic_bool closed;
    size_t mask;
    size_t limit;
    zb_message_t **slots;
    zb_wakeup_t *wakeup;
    zb_mutex_t mutex;
    zb_cond_t changed;
};

static void free_message(zb_message_t *message)
{
    free(message->data);
    free(message);
}

/* Wakes the I/O side only if it armed a wakeup before sleeping. */
static void notify_io(zb_channel_t *channel)
{
    if ((atomic_load(&channel->waiting) & IO_WAITING) != 0 &&
        (atomic_fetch_and(&channel->waiting, ~(unsigned)IO_WAITING) &
         IO_WAITING) != 0 &&
        channel->wakeup != NULL) {
        (void)zb_wakeup_signal(channel->wakeup);
    }
}

/* Wakes the worker side only if it sleeps on the condition variable. */
static void wake_worker(zb_channel_t *channel)
{
    (void)zb_mutex_lock(&channel->mutex);
    (void)zb_cond_broadcast(&channel->changed);
    (void)zb_mutex_unlock(&channel->mutex);
}

/* After I/O pushed: a waiting consumer can take the item. */
static void notify_consumer(zb_channel_t *channel)
{
    if ((atomic_load(&channel->waiting) & WORKER_WAITING) != 0) {
        wake_worker(channel);
    }
}

/* After I/O popped: a producer waiting on a full channel waits until half of
 * it has drained, so it is woken once per many messages, not per message. */
static void notify_producer(zb_channel_t *channel, size_t head)
{
    if ((atomic_load(&channel->waiting) & WORKER_WAITING) != 0 &&
        atomic_load(&channel->tail) - head <= channel->limit / 2) {
        wake_worker(channel);
    }
}

zerobus_status_t zb_channel_new(size_t max_messages, zb_wakeup_t *wakeup,
                                zb_channel_t **out_channel)
{
    if (!out_channel || !max_messages || max_messages > SIZE_MAX / 4) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    size_t capacity = 4;
    while (capacity < max_messages) {
        capacity *= 2;
    }
    zb_channel_t *channel = aligned_alloc(64, sizeof(*channel));
    if (!channel) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    memset(channel, 0, sizeof(*channel));
    channel->slots = calloc(capacity, sizeof(*channel->slots));
    if (!channel->slots) {
        free(channel);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status = zb_mutex_init(&channel->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }
    status = zb_cond_init(&channel->changed);
    if (status != ZEROBUS_STATUS_OK) {
        (void)zb_mutex_destroy(&channel->mutex);
        goto fail;
    }
    atomic_init(&channel->tail, 0);
    atomic_init(&channel->head, 0);
    atomic_init(&channel->waiting, 0);
    atomic_init(&channel->closed, false);
    channel->mask = capacity - 1;
    channel->limit = max_messages;
    channel->wakeup = wakeup;
    *out_channel = channel;
    return ZEROBUS_STATUS_OK;
fail:
    free(channel->slots);
    free(channel);
    return status;
}

void zb_channel_free(zb_channel_t *channel)
{
    if (!channel) {
        return;
    }
    size_t tail = atomic_load(&channel->tail);
    for (size_t i = atomic_load(&channel->head); i != tail; ++i) {
        free_message(channel->slots[i & channel->mask]);
    }
    (void)zb_cond_destroy(&channel->changed);
    (void)zb_mutex_destroy(&channel->mutex);
    free(channel->slots);
    free(channel);
}

/* Sleeps until ready() holds, the channel closes, or deadline passes.
 * Returns OK when ready, FAILED_PRECONDITION when closed first. */
static zerobus_status_t wait_worker(zb_channel_t *channel,
                                    bool (*ready)(zb_channel_t *),
                                    zb_deadline_t deadline)
{
    zerobus_status_t status = zb_mutex_lock(&channel->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    (void)atomic_fetch_or(&channel->waiting, WORKER_WAITING);
    for (;;) {
        if (ready(channel)) {
            status = ZEROBUS_STATUS_OK;
            break;
        }
        if (atomic_load(&channel->closed)) {
            status = ready(channel) ? ZEROBUS_STATUS_OK
                                    : ZEROBUS_STATUS_FAILED_PRECONDITION;
            break;
        }
        if (status != ZEROBUS_STATUS_OK) {
            break;
        }
        if (deadline == ZB_DEADLINE_IMMEDIATE) {
            status = ZEROBUS_STATUS_DEADLINE_EXCEEDED;
            break;
        }
        status =
            zb_cond_wait_until(&channel->changed, &channel->mutex, deadline);
    }
    (void)atomic_fetch_and(&channel->waiting, ~(unsigned)WORKER_WAITING);
    (void)zb_mutex_unlock(&channel->mutex);
    return status;
}

static bool has_space(zb_channel_t *channel)
{
    channel->head_seen = atomic_load(&channel->head);
    return atomic_load_explicit(&channel->tail, memory_order_relaxed) -
               channel->head_seen <
           channel->limit;
}

/* What a blocked producer waits for; see notify_producer. */
static bool half_drained(zb_channel_t *channel)
{
    channel->head_seen = atomic_load(&channel->head);
    return atomic_load_explicit(&channel->tail, memory_order_relaxed) -
               channel->head_seen <=
           channel->limit / 2;
}

static bool has_item(zb_channel_t *channel)
{
    channel->tail_seen = atomic_load(&channel->tail);
    return atomic_load_explicit(&channel->head, memory_order_relaxed) !=
           channel->tail_seen;
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
    size_t tail = atomic_load_explicit(&channel->tail, memory_order_relaxed);
    if (tail - channel->head_seen >= channel->limit && !has_space(channel)) {
        if (!blocking) {
            return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
        }
        if (deadline == ZB_DEADLINE_IMMEDIATE) {
            return ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        }
        zerobus_status_t status = wait_worker(channel, half_drained, deadline);
        if (status != ZEROBUS_STATUS_OK) {
            return status;
        }
        if (atomic_load(&channel->closed)) {
            return ZEROBUS_STATUS_FAILED_PRECONDITION;
        }
    }
    channel->slots[tail & channel->mask] = message;
    atomic_store(&channel->tail, tail + 1);
    /* The transfer is committed; a notification error cannot return
     * ownership. */
    if (blocking) {
        notify_io(channel);
    } else {
        notify_consumer(channel);
    }
    return ZEROBUS_STATUS_OK;
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
    size_t head = atomic_load_explicit(&channel->head, memory_order_relaxed);
    if (head == channel->tail_seen && !has_item(channel)) {
        /* Closing follows the producer's last push: recheck after it. */
        if (atomic_load(&channel->closed)) {
            if (!has_item(channel)) {
                *out_message = NULL;
                return ZEROBUS_STATUS_OK;
            }
        } else if (!blocking) {
            return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
        } else if (deadline == ZB_DEADLINE_IMMEDIATE) {
            return ZEROBUS_STATUS_DEADLINE_EXCEEDED;
        } else {
            zerobus_status_t status = wait_worker(channel, has_item, deadline);
            if (status == ZEROBUS_STATUS_FAILED_PRECONDITION) {
                *out_message = NULL;
                return ZEROBUS_STATUS_OK;
            }
            if (status != ZEROBUS_STATUS_OK) {
                return status;
            }
        }
    }
    zb_message_t *message = channel->slots[head & channel->mask];
    atomic_store(&channel->head, head + 1);
    if (blocking) {
        notify_io(channel);
    } else {
        notify_producer(channel, head + 1);
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

bool zb_channel_arm_pop_wakeup(zb_channel_t *channel)
{
    (void)atomic_fetch_or(&channel->waiting, IO_WAITING);
    return has_item(channel) || atomic_load(&channel->closed);
}

bool zb_channel_arm_push_wakeup(zb_channel_t *channel)
{
    (void)atomic_fetch_or(&channel->waiting, IO_WAITING);
    return has_space(channel) || atomic_load(&channel->closed);
}

zerobus_status_t zb_channel_close(zb_channel_t *channel)
{
    if (!channel) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    atomic_store(&channel->closed, true);
    (void)zb_mutex_lock(&channel->mutex);
    (void)zb_cond_broadcast(&channel->changed);
    (void)zb_mutex_unlock(&channel->mutex);
    return channel->wakeup ? zb_wakeup_signal(channel->wakeup)
                           : ZEROBUS_STATUS_OK;
}
