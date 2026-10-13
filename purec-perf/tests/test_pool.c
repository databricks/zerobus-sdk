#include "pool.h"
#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const size_t budget = 1025;

static void *exercise(void *argument) {
  _Atomic uint64_t *used = argument;
  for (unsigned i = 0; i < 10000; i++) {
    size_t size = 1 + i * 137 % budget;
    uint64_t reserved;
    while (!zbp_payload_reserve(used, budget, size, &reserved))
      sched_yield();
    assert(reserved >= size && reserved <= budget);
    assert(atomic_load_explicit(used, memory_order_relaxed) <= budget);
    unsigned char *payload = malloc(size);
    assert(payload);
    memset(payload, 0x5a, size);
    for (size_t j = 0; j < size; j++)
      assert(payload[j] == 0x5a);
    free(payload);
    zbp_payload_release(used, size);
  }
  return NULL;
}

int main(void) {
  _Atomic uint64_t used = 0;
  uint64_t reserved = 0;
  assert(zbp_payload_reserve(&used, budget, 513, &reserved));
  assert(reserved == 513);
  assert(zbp_payload_reserve(&used, budget, 512, &reserved));
  assert(reserved == budget);
  assert(!zbp_payload_reserve(&used, budget, 1, &reserved));
  assert(atomic_load(&used) == budget);
  zbp_payload_release(&used, 513);
  assert(zbp_payload_reserve(&used, budget, 513, &reserved));
  assert(reserved == budget);
  zbp_payload_release(&used, budget);
  assert(!zbp_payload_reserve(&used, budget, budget + 1, &reserved));
  assert(!atomic_load(&used));
  assert(zbp_payload_reserve(&used, SIZE_MAX, SIZE_MAX, &reserved));
  assert(reserved == SIZE_MAX);
  assert(!zbp_payload_reserve(&used, SIZE_MAX, 1, &reserved));
  zbp_payload_release(&used, SIZE_MAX);
  pthread_t threads[8];
  for (unsigned i = 0; i < 8; i++)
    assert(!pthread_create(&threads[i], NULL, exercise, &used));
  for (unsigned i = 0; i < 8; i++)
    assert(!pthread_join(threads[i], NULL));
  assert(!atomic_load(&used));
  puts("payload budget: exact capacity, reuse, overflow, concurrent bounds passed");
  return 0;
}
