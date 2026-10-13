#ifndef ZBP_QUEUE_H
#define ZBP_QUEUE_H
#include "zbperf.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
  _Atomic size_t sequence;
  void *value;
} zbp_slot;

typedef struct {
  _Alignas(64) _Atomic size_t tail;
  _Alignas(64) size_t head;
  _Alignas(64) _Atomic size_t count;
  _Atomic size_t peak;
  size_t capacity;
  zbp_queue_mode mode;
  zbp_slot *slots;
  pthread_mutex_t mutex;
} zbp_queue;

int zbp_queue_init(zbp_queue *queue, size_t capacity, zbp_queue_mode mode);
void zbp_queue_destroy(zbp_queue *queue);
int zbp_queue_push(zbp_queue *queue, void *value);
void *zbp_queue_peek(zbp_queue *queue);
void *zbp_queue_pop(zbp_queue *queue);
#endif
