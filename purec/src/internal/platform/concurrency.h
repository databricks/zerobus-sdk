#ifndef ZB_CONCURRENCY_H
#define ZB_CONCURRENCY_H

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include "zerobus/common.h"

/*
 * Absolute deadline on the monotonic clock, in nanoseconds.
 * The helpers convert relative timeouts: 0 means immediate, UINT64_MAX means
 * infinite, and finite results are capped at the backend's maximum deadline.
 */
typedef uint64_t zb_deadline_t;
#define ZB_DEADLINE_IMMEDIATE UINT64_C(0)
#define ZB_DEADLINE_INFINITE UINT64_MAX

zb_deadline_t zb_deadline_after_ms(uint64_t timeout_ms);
zb_deadline_t zb_deadline_after_seconds(uint64_t timeout_seconds);

/* Caller-owned storage. Access it through the wrapper functions below. */
typedef pthread_mutex_t zb_mutex_t;
typedef pthread_cond_t zb_cond_t;
typedef pthread_t zb_thread_t;
typedef pthread_once_t zb_once_t;
#define ZB_MUTEX_INITIALIZER PTHREAD_MUTEX_INITIALIZER
#define ZB_ONCE_INIT PTHREAD_ONCE_INIT

/* Initialize mutexes and condition variables before use, and destroy them
 * once all threads have finished using them. Both destroy functions accept
 * NULL. */
zerobus_status_t zb_mutex_init(zb_mutex_t *mutex);
zerobus_status_t zb_mutex_destroy(zb_mutex_t *mutex);

/* Mutexes are non-recursive. Unlock must run on the owning thread. */
zerobus_status_t zb_mutex_lock(zb_mutex_t *mutex);
zerobus_status_t zb_mutex_unlock(zb_mutex_t *mutex);

/* If already locked, returns OK with *out_locked == false. Failures leave
 * out_locked unchanged. */
zerobus_status_t zb_mutex_try_lock(zb_mutex_t *mutex, bool *out_locked);

zerobus_status_t zb_cond_init(zb_cond_t *cond);
zerobus_status_t zb_cond_destroy(zb_cond_t *cond);

/*
 * Call with mutex locked. Waiting atomically releases it, then reacquires it
 * before returning success or timeout; other errors have no such guarantee.
 * Concurrent waiters on the same condition must use the same mutex.
 */
zerobus_status_t zb_cond_wait(zb_cond_t *cond, zb_mutex_t *mutex);
zerobus_status_t zb_cond_wait_until(zb_cond_t *cond, zb_mutex_t *mutex,
                                    zb_deadline_t deadline);
zerobus_status_t zb_cond_signal(zb_cond_t *cond);
zerobus_status_t zb_cond_broadcast(zb_cond_t *cond);

/*
 * arg may be NULL; otherwise, keep it valid until join succeeds. The worker
 * may start before zb_thread_create returns. Data returned by the worker must
 * outlive the worker and is never freed by this layer.
 */
zerobus_status_t zb_thread_create(zb_thread_t *thread, void *(*entry)(void *),
                                  void *arg);

/*
 * Join from another thread, with only one caller joining this handle at a time.
 * Success waits for completion and makes the worker's writes visible. Failure
 * leaves out_result unchanged. On success, a non-NULL out_result receives the
 * worker's returned pointer. Pass NULL for out_result to ignore that pointer.
 */
zerobus_status_t zb_thread_join(zb_thread_t *thread, void **out_result);

/*
 * Declare the control with static storage duration and ZB_ONCE_INIT. Runs init
 * once; concurrent callers wait until it finishes.
 */
zerobus_status_t zb_once(zb_once_t *once, void (*init)(void));

#endif /* ZB_CONCURRENCY_H */
