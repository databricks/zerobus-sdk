#define _POSIX_C_SOURCE 200809L
#include "queue.h"
#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define PRODUCERS 8
#define ITEMS 30000
typedef struct {
  zbp_queue *queue;
  unsigned id;
} producer;

static void *produce(void *argument) {
  producer *p = argument;
  for (size_t i = 0; i < ITEMS; i++) {
    uintptr_t value = (uintptr_t)p->id * ITEMS + i + 1;
    while (!zbp_queue_push(p->queue, (void *)value))
      sched_yield();
  }
  return NULL;
}

static void run(zbp_queue_mode mode) {
  zbp_queue queue;
  assert(zbp_queue_init(&queue, 32, mode));
  for (uintptr_t n = 1; n <= 32; n++)
    assert(zbp_queue_push(&queue, (void *)n));
  assert(!zbp_queue_push(&queue, (void *)99));
  for (uintptr_t n = 1; n <= 32; n++) {
    assert(zbp_queue_peek(&queue) == (void *)n);
    assert(zbp_queue_pop(&queue) == (void *)n);
  }
  assert(!zbp_queue_pop(&queue));
  size_t expected[PRODUCERS] = {0};
  pthread_t threads[PRODUCERS];
  producer producers[PRODUCERS];
  for (unsigned n = 0; n < PRODUCERS; n++) {
    producers[n] = (producer){&queue, n};
    assert(!pthread_create(&threads[n], NULL, produce, &producers[n]));
  }
  for (size_t n = 0; n < PRODUCERS * ITEMS;) {
    uintptr_t value = (uintptr_t)zbp_queue_pop(&queue);
    if (!value) {
      sched_yield();
      continue;
    }
    value--;
    unsigned id = (unsigned)(value / ITEMS);
    assert(id < PRODUCERS);
    assert(value % ITEMS == expected[id]++);
    n++;
  }
  for (unsigned n = 0; n < PRODUCERS; n++)
    assert(!pthread_join(threads[n], NULL));
  assert(!atomic_load(&queue.count));
  assert(atomic_load(&queue.peak) <= 32);
  zbp_queue_destroy(&queue);
}

static void interrupted_publication(void) {
  zbp_queue queue;
  assert(zbp_queue_init(&queue, 2, ZBP_QUEUE_ATOMIC));
  /* Model a descheduled producer after reserving slot zero. */
  atomic_store(&queue.tail, 1);
  assert(zbp_queue_push(&queue, (void *)2));
  assert(!zbp_queue_pop(&queue));
  assert(!zbp_queue_push(&queue, (void *)3));
  queue.slots[0].value = (void *)1;
  atomic_fetch_add(&queue.count, 1);
  atomic_store_explicit(&queue.slots[0].sequence, 1, memory_order_release);
  assert(zbp_queue_pop(&queue) == (void *)1);
  assert(zbp_queue_pop(&queue) == (void *)2);
  assert(zbp_queue_push(&queue, (void *)3));
  assert(zbp_queue_pop(&queue) == (void *)3);
  zbp_queue_destroy(&queue);
}

int main(void) {
  zbp_queue invalid;
  assert(!zbp_queue_init(&invalid, 3, ZBP_QUEUE_MUTEX));
  run(ZBP_QUEUE_MUTEX);
  run(ZBP_QUEUE_ATOMIC);
  interrupted_publication();
  puts("queue: wraparound, MPSC ordering, bounded occupancy, interrupted "
       "publication passed");
  return 0;
}
