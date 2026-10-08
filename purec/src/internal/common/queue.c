#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "internal/log.h"
#include "queue.h"

/*
 * Vyukov's bounded MPMC queue with per-slot sequence numbers, extended from
 * two stages (push, pop) to three (push, advance, pop).
 *
 * How the queue looks. Example: capacity 8, after 18 pushes, 15 advances and
 * 13 pops.
 *
 * Positions count up forever, one per pushed item. The three counters split
 * them into regions: [head, cursor) is the second part, [cursor, tail) the
 * first part, and [tail, head + capacity) the free slots. The counters never
 * pass each other, and tail never gets more than capacity ahead of head:
 * head <= cursor <= tail <= head + capacity.
 *
 *                    head      cursor         tail
 *                    v         v              v
 *    ... | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | ...
 *                   \__2nd__/ \____1st_____/ \____free____/
 *         popped                                            next lap
 *
 * Position 21 is the next lap of slot 5, which still holds position 13.
 *
 * In memory, position p lives in slot p % capacity, so the regions wrap around
 * the end of the array.
 *
 * Each slot also carries a sequence number, seq, which encodes the slot's
 * stage. For the slot of position p:
 *
 *   seq == p             free: waiting for the push at the position p
 *   seq == p + 1         in the first part: pushed, waiting to be advanced
 *   seq == p + 2         in the second part: advanced, waiting to be popped
 *   seq == p + capacity  popped: free again, now for position p + capacity
 *
 * Each slot cycles through first part, second part and free, and its seq only
 * ever increases. In the diagram below, a free slot shows the position that
 * will use it next.
 *
 *    slot        0    1    2    3    4    5    6    7
 *              +----+----+----+----+----+----+----+----+
 *    position  | 16 | 17 | 18 | 19 | 20 | 13 | 14 | 15 |
 *    seq       | 17 | 18 | 18 | 19 | 20 | 15 | 16 | 16 |
 *              +----+----+----+----+----+----+----+----+
 *               1st  1st  free free free 2nd  2nd  1st
 *                          ^              ^         ^
 *                          tail           head      cursor
 *
 * Each operation claims the position under its own counter with a CAS, but
 * only once that position's slot is in the stage the operation requires: free
 * for push, first part for advance, second part for pop. It then hands the
 * slot to the next stage with a release store of its seq:
 *
 *   push     claims tail    when seq == tail,       then seq = tail + 1
 *   advance  claims cursor  when seq == cursor + 1, then seq = cursor + 2
 *   pop      claims head    when seq == head + 2,   then seq = head + capacity
 *
 * In the example, push would take slot 2 (seq 18 == tail), advance slot 7
 * (seq 16 == cursor + 1), and pop slot 5 (seq 15 == head + 2). After that pop,
 * slot 5 holds seq 21: free, for position 21.
 *
 * When the slot under a counter is not in the required stage yet, the
 * operation returns. This happens when:
 *   - full: tail == head + capacity, so the slot under tail still holds
 *     position head (seq head + 1 or head + 2, behind tail);
 *   - first part empty: cursor == tail, so position cursor has not been pushed
 *     yet (its slot's seq is at most cursor, behind cursor + 1);
 *   - second part empty: head == cursor, and the slot under head is free or
 *     in the first part (seq head or head + 1, behind head + 2);
 *   - in flight: another operation has moved its counter but not yet stored
 *     the new seq, so the slot still reads as the previous stage.
 */

enum { STAGE_PUSH = 0, STAGE_ADVANCE = 1, STAGE_POP = 2 };

/* Pad each counter to its own cache line to avoid false sharing between the
 * push, advance, and pop threads. 64 holds on x86-64, other CPUs differ
 * (128 on Apple Silicon and POWER).
 * Override per target with -DZB_CACHE_LINE=... */
#ifndef ZB_CACHE_LINE
#define ZB_CACHE_LINE 64
#endif

struct slot {
    atomic_size_t seq;
    void *item; /* borrowed */
};

struct zb_queue {
    struct slot *slots;
    size_t capacity; /* a power of two */

    /* Keep each counter on its own cache line. */
    char pad_config[ZB_CACHE_LINE];
    atomic_size_t head; /* next position to pop */
    char pad_head[ZB_CACHE_LINE - sizeof(atomic_size_t)];
    atomic_size_t cursor; /* next position to advance */
    char pad_cursor[ZB_CACHE_LINE - sizeof(atomic_size_t)];
    atomic_size_t tail; /* next position to push */
};

/* True when seq has not reached expected yet (modular comparison). */
static bool is_behind(size_t seq, size_t expected)
{
    return seq - expected > SIZE_MAX / 2;
}

/*
 * Claim the next position of counter once its slot reaches stage. NULL when
 * that slot is behind, i.e. the previous stage has not finished with it.
 */
static struct slot *claim(zb_queue_t *queue, atomic_size_t *counter,
                          size_t stage, size_t *out_position)
{
    for (;;) {
        size_t position = atomic_load_explicit(counter, memory_order_relaxed);
        struct slot *slot = &queue->slots[position & (queue->capacity - 1)];
        size_t seq = atomic_load_explicit(&slot->seq, memory_order_acquire);
        if (is_behind(seq, position + stage)) {
            return NULL;
        }
        if (seq == position + stage &&
            atomic_compare_exchange_weak_explicit(
                counter, &position, position + 1, memory_order_relaxed,
                memory_order_relaxed)) {
            *out_position = position;
            return slot;
        }
    }
}

zerobus_status_t zb_queue_new(size_t capacity, zb_queue_t **out_queue)
{
    if (out_queue == NULL || capacity < 4 || (capacity & (capacity - 1)) != 0 ||
        capacity > SIZE_MAX / sizeof(struct slot)) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zb_queue_t *queue = (zb_queue_t *)calloc(1, sizeof(*queue));
    struct slot *slots = (struct slot *)calloc(capacity, sizeof(*slots));
    if (queue == NULL || slots == NULL) {
        ZB_ERROR("queue allocation failed");
        free(slots);
        free(queue);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    for (size_t i = 0; i < capacity; i++) {
        atomic_init(&slots[i].seq, i);
    }
    queue->slots = slots;
    queue->capacity = capacity;
    atomic_init(&queue->head, 0);
    atomic_init(&queue->cursor, 0);
    atomic_init(&queue->tail, 0);
    *out_queue = queue;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_queue_push(zb_queue_t *queue, void *item,
                               size_t *out_position)
{
    if (queue == NULL || item == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    size_t position;
    struct slot *slot = claim(queue, &queue->tail, STAGE_PUSH, &position);
    if (slot == NULL) {
        return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    }
    slot->item = item;
    atomic_store_explicit(&slot->seq, position + STAGE_ADVANCE,
                          memory_order_release);
    if (out_position != NULL) {
        *out_position = position;
    }
    return ZEROBUS_STATUS_OK;
}

void *zb_queue_advance(zb_queue_t *queue, size_t *out_position)
{
    if (queue == NULL) {
        return NULL;
    }
    size_t position;
    struct slot *slot = claim(queue, &queue->cursor, STAGE_ADVANCE, &position);
    if (slot == NULL) {
        return NULL;
    }
    void *item = slot->item;
    atomic_store_explicit(&slot->seq, position + STAGE_POP,
                          memory_order_release);
    if (out_position != NULL) {
        *out_position = position;
    }
    return item;
}

void *zb_queue_peek(const zb_queue_t *queue, size_t *out_position)
{
    if (queue == NULL) {
        return NULL;
    }
    size_t position = atomic_load_explicit(&queue->head, memory_order_relaxed);
    const struct slot *slot = &queue->slots[position & (queue->capacity - 1)];
    size_t seq = atomic_load_explicit(&slot->seq, memory_order_acquire);
    if (seq != position + STAGE_POP) {
        return NULL;
    }
    if (out_position != NULL) {
        *out_position = position;
    }
    return slot->item;
}

void *zb_queue_pop(zb_queue_t *queue, size_t *out_position)
{
    if (queue == NULL) {
        return NULL;
    }
    size_t position;
    struct slot *slot = claim(queue, &queue->head, STAGE_POP, &position);
    if (slot == NULL) {
        return NULL;
    }
    void *item = slot->item;
    atomic_store_explicit(&slot->seq, position + queue->capacity,
                          memory_order_release);
    if (out_position != NULL) {
        *out_position = position;
    }
    return item;
}

void zb_queue_free(zb_queue_t *queue)
{
    if (queue == NULL) {
        return;
    }
    free(queue->slots);
    free(queue);
}
