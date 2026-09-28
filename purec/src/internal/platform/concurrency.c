/* Keep POSIX declarations local without changing the project's C99 baseline. */
#define _POSIX_C_SOURCE 200809L

#include <errno.h> // IWYU pragma: keep
#include <limits.h>
#include <pthread.h>
#include <time.h>

#include "concurrency.h"

#define ZB_NS_PER_SECOND UINT64_C(1000000000)
#define ZB_NS_PER_MS UINT64_C(1000000)
#define ZB_MS_PER_SECOND UINT64_C(1000)

static zerobus_status_t backend_status(int result)
{
    switch (result) {
    case 0:
        return ZEROBUS_STATUS_OK;
    case ENOMEM:
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    case EAGAIN:
        return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    case EINVAL:
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    case EBUSY:
    case EDEADLK:
    case ESRCH:
        return ZEROBUS_STATUS_FAILED_PRECONDITION;
    case EPERM:
        return ZEROBUS_STATUS_PERMISSION_DENIED;
    default:
        return ZEROBUS_STATUS_INTERNAL;
    }
}

static zb_deadline_t max_deadline(void)
{
    unsigned int time_bits = sizeof(time_t) * CHAR_BIT;
    /* Use the signed range conservatively even on unsigned time_t hosts. */
    uint64_t max_seconds =
        time_bits <= 64 ? (UINT64_C(1) << (time_bits - 1)) - 1 : UINT64_MAX;
    if (max_seconds >= (UINT64_MAX - 1) / ZB_NS_PER_SECOND) {
        return UINT64_MAX - 1;
    }
    return max_seconds * ZB_NS_PER_SECOND + ZB_NS_PER_SECOND - 1;
}

zb_deadline_t zb_deadline_after_ms(uint64_t timeout_ms)
{
    if (timeout_ms == UINT64_MAX) {
        return ZB_DEADLINE_INFINITE;
    }
    if (timeout_ms == 0) {
        return ZB_DEADLINE_IMMEDIATE;
    }
    struct timespec now;
    (void)clock_gettime(CLOCK_MONOTONIC, &now);
    zb_deadline_t limit = max_deadline();
    uint64_t seconds = (uint64_t)now.tv_sec;
    if (seconds > limit / ZB_NS_PER_SECOND) {
        return limit;
    }
    uint64_t elapsed = seconds * ZB_NS_PER_SECOND;
    if ((uint64_t)now.tv_nsec > limit - elapsed) {
        return limit;
    }
    elapsed += (uint64_t)now.tv_nsec;
    if (timeout_ms > (limit - elapsed) / ZB_NS_PER_MS) {
        return limit;
    }
    return elapsed + timeout_ms * ZB_NS_PER_MS;
}

zb_deadline_t zb_deadline_after_seconds(uint64_t timeout_seconds)
{
    if (timeout_seconds == UINT64_MAX) {
        return ZB_DEADLINE_INFINITE;
    }
    if (timeout_seconds > (UINT64_MAX - 1) / ZB_MS_PER_SECOND) {
        return max_deadline();
    }
    return zb_deadline_after_ms(timeout_seconds * ZB_MS_PER_SECOND);
}

zerobus_status_t zb_mutex_init(zb_mutex_t *mutex)
{
    if (mutex == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    pthread_mutexattr_t attr;
    int result = pthread_mutexattr_init(&attr);
    if (result != 0) {
        return backend_status(result);
    }
    result = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_NORMAL);
    if (result == 0) {
        result = pthread_mutex_init(mutex, &attr);
    }
    (void)pthread_mutexattr_destroy(&attr);
    return backend_status(result);
}

zerobus_status_t zb_mutex_destroy(zb_mutex_t *mutex)
{
    if (mutex == NULL) {
        return ZEROBUS_STATUS_OK;
    }
    return backend_status(pthread_mutex_destroy(mutex));
}

zerobus_status_t zb_mutex_lock(zb_mutex_t *mutex)
{
    if (mutex == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    return backend_status(pthread_mutex_lock(mutex));
}

zerobus_status_t zb_mutex_unlock(zb_mutex_t *mutex)
{
    if (mutex == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    return backend_status(pthread_mutex_unlock(mutex));
}

zerobus_status_t zb_mutex_try_lock(zb_mutex_t *mutex, bool *out_locked)
{
    if (mutex == NULL || out_locked == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    int result = pthread_mutex_trylock(mutex);
    if (result != 0 && result != EBUSY) {
        return backend_status(result);
    }
    *out_locked = result == 0;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_cond_init(zb_cond_t *cond)
{
    if (cond == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    pthread_condattr_t attr;
    int result = pthread_condattr_init(&attr);
    if (result != 0) {
        return backend_status(result);
    }
    result = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    if (result == 0) {
        result = pthread_cond_init(cond, &attr);
    }
    (void)pthread_condattr_destroy(&attr);
    return backend_status(result);
}

zerobus_status_t zb_cond_destroy(zb_cond_t *cond)
{
    if (cond == NULL) {
        return ZEROBUS_STATUS_OK;
    }
    return backend_status(pthread_cond_destroy(cond));
}

zerobus_status_t zb_cond_wait(zb_cond_t *cond, zb_mutex_t *mutex)
{
    return zb_cond_wait_until(cond, mutex, ZB_DEADLINE_INFINITE);
}

zerobus_status_t zb_cond_wait_until(zb_cond_t *cond, zb_mutex_t *mutex,
                                    zb_deadline_t deadline)
{
    if (cond == NULL || mutex == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    int result;
    if (deadline == ZB_DEADLINE_INFINITE) {
        result = pthread_cond_wait(cond, mutex);
    } else {
        zb_deadline_t limit = max_deadline();
        if (deadline > limit) {
            deadline = limit;
        }
        struct timespec until = {
            .tv_sec = (time_t)(deadline / ZB_NS_PER_SECOND),
            .tv_nsec = (long)(deadline % ZB_NS_PER_SECOND),
        };
        result = pthread_cond_timedwait(cond, mutex, &until);
    }
    if (result == ETIMEDOUT) {
        return ZEROBUS_STATUS_DEADLINE_EXCEEDED;
    }
    return backend_status(result);
}

zerobus_status_t zb_cond_signal(zb_cond_t *cond)
{
    if (cond == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    return backend_status(pthread_cond_signal(cond));
}

zerobus_status_t zb_cond_broadcast(zb_cond_t *cond)
{
    if (cond == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    return backend_status(pthread_cond_broadcast(cond));
}

zerobus_status_t zb_thread_create(zb_thread_t *thread, void *(*entry)(void *),
                                  void *arg)
{
    if (thread == NULL || entry == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    return backend_status(pthread_create(thread, NULL, entry, arg));
}

zerobus_status_t zb_thread_join(zb_thread_t *thread, void **out_result)
{
    if (thread == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    void *worker_result = NULL;
    int result = pthread_join(*thread, &worker_result);
    if (result != 0) {
        return backend_status(result);
    }
    if (out_result != NULL) {
        *out_result = worker_result;
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_once(zb_once_t *once, void (*init)(void))
{
    if (once == NULL || init == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    return backend_status(pthread_once(once, init));
}
