#ifndef ZB_QUEUE_H
#define ZB_QUEUE_H

#include <stddef.h>

#include "zerobus/common.h"

/*
 * Bounded MPMC FIFO of opaque, non-NULL items, split into two parts, each in
 * push order. Items enter the first part, move to the second (advancing), and
 * leave from there, so every item in the second part is older than every item
 * in the first. Both parts share the capacity.
 *
 * push, advance and pop may each run concurrently from any number of threads.
 * peek must not run concurrently with pop (see peek).
 * new and free are exclusive.
 *
 * An item's position is 0 for the first item pushed, then consecutive.
 * Positions are monotonic, not modular: they keep increasing for the queue's
 * whole life and never wrap at the capacity. Every function below that takes
 * an out_position writes the item's position there when out_position is not
 * NULL, and leaves it unchanged when it returns no item.
 */
typedef struct zb_queue zb_queue_t;

/*
 * capacity must be a power of two, at least 4. All slots are allocated up
 * front. INVALID_ARGUMENT for a NULL out_queue or a bad capacity. Failures
 * leave *out_queue unchanged.
 */
zerobus_status_t zb_queue_new(size_t capacity, zb_queue_t **out_queue);

/*
 * On success the queue holds item until pop hands it back. A failure takes no
 * position. INVALID_ARGUMENT for a NULL queue or item, RESOURCE_EXHAUSTED when
 * full. Advanced items occupy slots too: only pop frees one.
 */
zerobus_status_t zb_queue_push(zb_queue_t *queue, void *item,
                               size_t *out_position);

/* The oldest item not advanced yet, now advanced and still held by the
 * queue. NULL when no item is waiting to advance, including one whose push
 * has not published it yet, also NULL for a NULL queue.
 *
 * The returned pointer can dangle if a concurrent pop's caller frees the item.
 * Callers that dereference it must coordinate that. */
void *zb_queue_advance(zb_queue_t *queue, size_t *out_position);

/* The oldest advanced item, still held by the queue. NULL when no advanced
 * item is waiting, also NULL for a NULL queue.
 *
 * Must not run concurrently with pop: a concurrent pop frees and recycles the
 * slot, so peek can read a torn or replaced item, or return a position that no
 * longer matches the item (a data race on queue storage). Any number of
 * concurrent push and advance calls are fine. */
void *zb_queue_peek(const zb_queue_t *queue, size_t *out_position);

/* Remove the oldest advanced item and hand it to the caller. NULL when no
 * advanced item is waiting, including one whose advance has not published it
 * yet, also NULL for a NULL queue. */
void *zb_queue_pop(zb_queue_t *queue, size_t *out_position);

/* Frees the queue but not the items it still holds, those remain the caller's.
 * To get them back, drain first: advance until NULL, then pop until NULL.
 * Accepts NULL. */
void zb_queue_free(zb_queue_t *queue);

#endif /* ZB_QUEUE_H */
