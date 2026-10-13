#include "queue.h"
#include <stdlib.h>

int zbp_queue_init(zbp_queue *q, size_t capacity, zbp_queue_mode mode) {
  if (capacity < 2 || (capacity & (capacity - 1)) ||
      capacity > SIZE_MAX / sizeof(zbp_slot))
    return 0;
  q->slots = calloc(capacity, sizeof(*q->slots));
  if (!q->slots)
    return 0;
  q->capacity = capacity;
  q->mode = mode;
  q->head = 0;
  atomic_init(&q->tail, 0);
  atomic_init(&q->count, 0);
  atomic_init(&q->peak, 0);
  if (pthread_mutex_init(&q->mutex, NULL)) {
    free(q->slots);
    return 0;
  }
  for (size_t i = 0; i < capacity; i++)
    atomic_init(&q->slots[i].sequence, i);
  return 1;
}

void zbp_queue_destroy(zbp_queue *q) {
  pthread_mutex_destroy(&q->mutex);
  free(q->slots);
}

static void increment_count(zbp_queue *q) {
  size_t count =
      atomic_fetch_add_explicit(&q->count, 1, memory_order_relaxed) + 1;
  size_t peak = atomic_load_explicit(&q->peak, memory_order_relaxed);
  while (count > peak && !atomic_compare_exchange_weak_explicit(
                             &q->peak, &peak, count, memory_order_relaxed,
                             memory_order_relaxed)) {
  }
}

int zbp_queue_push(zbp_queue *q, void *value) {
  if (q->mode == ZBP_QUEUE_MUTEX) {
    pthread_mutex_lock(&q->mutex);
    size_t pos = atomic_load_explicit(&q->tail, memory_order_relaxed);
    if (pos - q->head == q->capacity) {
      pthread_mutex_unlock(&q->mutex);
      return 0;
    }
    q->slots[pos & (q->capacity - 1)].value = value;
    increment_count(q);
    atomic_store_explicit(&q->tail, pos + 1, memory_order_relaxed);
    pthread_mutex_unlock(&q->mutex);
    return 1;
  }
  size_t pos = atomic_load_explicit(&q->tail, memory_order_relaxed);
  zbp_slot *slot;
  for (;;) {
    slot = &q->slots[pos & (q->capacity - 1)];
    size_t sequence =
        atomic_load_explicit(&slot->sequence, memory_order_acquire);
    intptr_t difference = (intptr_t)(sequence - pos);
    if (difference == 0) {
      if (atomic_compare_exchange_weak_explicit(&q->tail, &pos, pos + 1,
                                                memory_order_relaxed,
                                                memory_order_relaxed))
        break;
    } else if (difference < 0)
      return 0;
    else
      pos = atomic_load_explicit(&q->tail, memory_order_relaxed);
  }
  /* All allocation and copying precedes reservation; publication cannot fail.
   */
  slot->value = value;
  increment_count(q);
  atomic_store_explicit(&slot->sequence, pos + 1, memory_order_release);
  return 1;
}

void *zbp_queue_peek(zbp_queue *q) {
  if (q->mode == ZBP_QUEUE_MUTEX) {
    pthread_mutex_lock(&q->mutex);
    void *value =
        q->head == atomic_load_explicit(&q->tail, memory_order_relaxed)
            ? NULL
            : q->slots[q->head & (q->capacity - 1)].value;
    pthread_mutex_unlock(&q->mutex);
    return value;
  }
  zbp_slot *slot = &q->slots[q->head & (q->capacity - 1)];
  return atomic_load_explicit(&slot->sequence, memory_order_acquire) ==
                 q->head + 1
             ? slot->value
             : NULL;
}

void *zbp_queue_pop(zbp_queue *q) {
  if (q->mode == ZBP_QUEUE_MUTEX) {
    pthread_mutex_lock(&q->mutex);
    if (q->head == atomic_load_explicit(&q->tail, memory_order_relaxed)) {
      pthread_mutex_unlock(&q->mutex);
      return NULL;
    }
    void *value = q->slots[q->head++ & (q->capacity - 1)].value;
    atomic_fetch_sub_explicit(&q->count, 1, memory_order_relaxed);
    pthread_mutex_unlock(&q->mutex);
    return value;
  }
  zbp_slot *slot = &q->slots[q->head & (q->capacity - 1)];
  if (atomic_load_explicit(&slot->sequence, memory_order_acquire) !=
      q->head + 1)
    return NULL;
  void *value = slot->value;
  atomic_fetch_sub_explicit(&q->count, 1, memory_order_relaxed);
  atomic_store_explicit(&slot->sequence, q->head + q->capacity,
                        memory_order_release);
  q->head++;
  return value;
}
