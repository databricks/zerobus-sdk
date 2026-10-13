#include "pool.h"

int zbp_payload_reserve(_Atomic uint64_t *used, size_t budget, size_t size,
                        uint64_t *reserved) {
  if (size > budget)
    return 0;
  uint64_t old = atomic_load_explicit(used, memory_order_relaxed);
  while (old <= budget - size) {
    if (atomic_compare_exchange_weak_explicit(
            used, &old, old + size, memory_order_relaxed, memory_order_relaxed)) {
      *reserved = old + size;
      return 1;
    }
  }
  return 0;
}

void zbp_payload_release(_Atomic uint64_t *used, size_t size) {
  atomic_fetch_sub_explicit(used, size, memory_order_relaxed);
}
