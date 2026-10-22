#define _POSIX_C_SOURCE 200809L

#include <errno.h> // IWYU pragma: keep
#include <limits.h>
#include <pthread.h>
#include <sched.h>
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

static struct timespec deadline_timespec(zb_deadline_t deadline)
{
    zb_deadline_t limit = max_deadline();
    if (deadline > limit) {
        deadline = limit;
    }
    return (struct timespec){
        .tv_sec = (time_t)(deadline / ZB_NS_PER_SECOND),
        .tv_nsec = (long)(deadline % ZB_NS_PER_SECOND),
    };
}

static zb_deadline_t now(void)
{
    struct timespec current;
    (void)clock_gettime(CLOCK_MONOTONIC, &current);
    zb_deadline_t limit = max_deadline();
    uint64_t seconds = (uint64_t)current.tv_sec;
    if (seconds > limit / ZB_NS_PER_SECOND) {
        return limit;
    }
    uint64_t elapsed = seconds * ZB_NS_PER_SECOND;
    if ((uint64_t)current.tv_nsec > limit - elapsed) {
        return limit;
    }
    return elapsed + (uint64_t)current.tv_nsec;
}

zb_deadline_t zb_deadline_after_ms(uint64_t timeout_ms)
{
    if (timeout_ms == UINT64_MAX) {
        return ZB_DEADLINE_INFINITE;
    }
    if (timeout_ms == 0) {
        return ZB_DEADLINE_IMMEDIATE;
    }
    zb_deadline_t limit = max_deadline();
    zb_deadline_t start = now();
    if (timeout_ms > (limit - start) / ZB_NS_PER_MS) {
        return limit;
    }
    return start + timeout_ms * ZB_NS_PER_MS;
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

uint64_t zb_deadline_remaining_ms(zb_deadline_t deadline)
{
    if (deadline == ZB_DEADLINE_INFINITE) {
        return UINT64_MAX;
    }
    zb_deadline_t current = now();
    if (deadline <= current) {
        return 0;
    }
    return (deadline - current - 1) / ZB_NS_PER_MS + 1;
}

bool zb_deadline_expired(zb_deadline_t deadline)
{
    return now() >= deadline;
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
        struct timespec until = deadline_timespec(deadline);
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

zerobus_status_t zb_sem_init(zb_sem_t *sem, unsigned int value)
{
    if (sem == NULL || value > ZB_SEM_VALUE_MAX) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zerobus_status_t status = zb_mutex_init(&sem->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    status = zb_cond_init(&sem->cond);
    if (status != ZEROBUS_STATUS_OK) {
        (void)zb_mutex_destroy(&sem->mutex);
        return status;
    }
    atomic_init(&sem->count, value);
    atomic_init(&sem->waiters, 0);
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_sem_destroy(zb_sem_t *sem)
{
    if (sem == NULL) {
        return ZEROBUS_STATUS_OK;
    }
    zerobus_status_t status = zb_cond_destroy(&sem->cond);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    return zb_mutex_destroy(&sem->mutex);
}

/* Takes a permit if one is available. */
static bool sem_take(zb_sem_t *sem)
{
    unsigned int count = atomic_load(&sem->count);
    while (count != 0) {
        if (atomic_compare_exchange_weak(&sem->count, &count, count - 1)) {
            return true;
        }
    }
    return false;
}

/* A sleeper publishes itself in waiters before rechecking count, and post
 * publishes its permit before reading waiters: one of them sees the other.
 * Post signals under the mutex, which the sleeper holds from publishing until
 * it waits, so the signal cannot fall between its check and its wait. */
zerobus_status_t zb_sem_post(zb_sem_t *sem)
{
    if (sem == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    unsigned int count = atomic_load(&sem->count);
    do {
        if (count == ZB_SEM_VALUE_MAX) {
            return ZEROBUS_STATUS_INTERNAL;
        }
    } while (!atomic_compare_exchange_weak(&sem->count, &count, count + 1));
    if (atomic_load(&sem->waiters) == 0) {
        return ZEROBUS_STATUS_OK;
    }
    zerobus_status_t status = zb_mutex_lock(&sem->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    status = zb_cond_signal(&sem->cond);
    zerobus_status_t unlock_status = zb_mutex_unlock(&sem->mutex);
    return status == ZEROBUS_STATUS_OK ? unlock_status : status;
}

zerobus_status_t zb_sem_wait(zb_sem_t *sem, zb_deadline_t deadline)
{
    if (sem == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (sem_take(sem)) {
        return ZEROBUS_STATUS_OK;
    }
    if (deadline == ZB_DEADLINE_IMMEDIATE) {
        return ZEROBUS_STATUS_DEADLINE_EXCEEDED;
    }
    zerobus_status_t status = zb_mutex_lock(&sem->mutex);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    (void)atomic_fetch_add(&sem->waiters, 1);
    bool taken = false;
    for (;;) {
        if (sem_take(sem)) {
            taken = true;
            break;
        }
        if (status != ZEROBUS_STATUS_OK) {
            break; /* Timed out or failed, and still no permit. */
        }
        status = zb_cond_wait_until(&sem->cond, &sem->mutex, deadline);
    }
    (void)atomic_fetch_sub(&sem->waiters, 1);
    zerobus_status_t unlock_status = zb_mutex_unlock(&sem->mutex);
    return taken ? unlock_status : status;
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

void zb_thread_yield(void)
{
    (void)sched_yield();
}

void zb_sleep_us(uint64_t microseconds)
{
    struct timespec delay = {
        .tv_sec = (time_t)(microseconds / 1000000u),
        .tv_nsec = (long)(microseconds % 1000000u) * 1000L,
    };
    while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
    }
}
