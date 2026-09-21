/* Offline concurrency tests coordinate through predicates, never sleeps. */
#include <stdlib.h>

#include "internal/concurrency.h"
#include "test_common.h"

enum { WORKERS = 4, INCREMENTS = 1000, WAIT_TIMEOUT_MS = 5000 };

/* A failed join must not let a worker outlive its borrowed stack state. */
static void join_owned(zb_thread_t **thread)
{
    if (*thread != NULL) {
        zerobus_status_t status = zb_thread_join(*thread, NULL);
        if (status != ZEROBUS_STATUS_OK) {
            fprintf(stderr, "Thread cleanup join failed (status %u).\n",
                    (unsigned int)status);
            abort();
        }
        *thread = NULL;
    }
}

/* Publish a value before returning the caller-owned pointer unchanged. */
static void *set_value(void *arg)
{
    if (arg != NULL) {
        *(int *)arg = 42;
    }
    return arg;
}

/* Carry one worker's try-lock result back to the joining thread. */
struct try_context {
    zb_mutex_t *mutex;
    zerobus_status_t status;
    bool acquired;
};

/* Try once and release the mutex if it was unexpectedly available. */
static void *try_mutex(void *arg)
{
    struct try_context *context = (struct try_context *)arg;
    context->status = zb_mutex_try_lock(context->mutex, &context->acquired);
    if (context->status == ZEROBUS_STATUS_OK && context->acquired) {
        context->status = zb_mutex_unlock(context->mutex);
    }
    return NULL;
}

/* Probe ownership from another thread without waiting for the mutex. */
static struct try_context try_from_thread(zb_mutex_t *mutex)
{
    struct try_context context = {mutex, ZEROBUS_STATUS_UNKNOWN, true};
    zb_thread_t *thread = NULL;
    zerobus_status_t status = zb_thread_new(try_mutex, &context, &thread);
    if (status != ZEROBUS_STATUS_OK) {
        context.status = status;
    }
    join_owned(&thread);
    return context;
}

/* The counter is shared only while its mutex is held. */
struct counter {
    zb_mutex_t *mutex;
    unsigned int value;
};

/* Each counter worker reports errors without touching test assertions. */
struct counter_context {
    struct counter *counter;
    zerobus_status_t status;
};

/* Perform a bounded set of mutex-protected increments. */
static void *increment_counter(void *arg)
{
    struct counter_context *context = (struct counter_context *)arg;
    for (unsigned int i = 0; i < INCREMENTS; i++) {
        context->status = zb_mutex_lock(context->counter->mutex);
        if (context->status != ZEROBUS_STATUS_OK) {
            return NULL;
        }
        context->counter->value++;
        context->status = zb_mutex_unlock(context->counter->mutex);
        if (context->status != ZEROBUS_STATUS_OK) {
            return NULL;
        }
    }
    return NULL;
}

/* Exercise both wait entry points and finite deadline conversion. */
enum wait_mode { WAIT_UNTIMED, WAIT_INFINITE, WAIT_FINITE, WAIT_LARGE };

/* Keep per-worker results separate from mutex-protected shared state. */
struct wait_context {
    struct wait_fixture *fixture;
    enum wait_mode mode;
    zerobus_status_t status;
};

/* Own the condition scenario, including partially started workers. */
struct wait_fixture {
    zb_mutex_t *mutex;
    zb_cond_t *changed;
    zb_cond_t *progress;
    unsigned int ready;
    unsigned int wakeups;
    unsigned int completed;
    bool proceed;
    bool stop;
    bool holding;
    unsigned int created;
    struct wait_context contexts[WORKERS];
    zb_thread_t *threads[WORKERS];
};

/* Recheck the predicate after every notification and report progress. */
static void *wait_for_permission(void *arg)
{
    struct wait_context *context = (struct wait_context *)arg;
    struct wait_fixture *fixture = context->fixture;
    zb_deadline_t deadline = ZB_DEADLINE_INFINITE;
    if (context->mode == WAIT_FINITE) {
        deadline = zb_deadline_after_ms(WAIT_TIMEOUT_MS);
    } else if (context->mode == WAIT_LARGE) {
        deadline = zb_deadline_after_ms(UINT64_MAX - 1);
    }

    context->status = zb_mutex_lock(fixture->mutex);
    if (context->status != ZEROBUS_STATUS_OK) {
        return NULL;
    }
    fixture->ready++;
    context->status = zb_cond_broadcast(fixture->progress);
    while (context->status == ZEROBUS_STATUS_OK && !fixture->proceed &&
           !fixture->stop) {
        context->status = context->mode == WAIT_UNTIMED
                              ? zb_cond_wait(fixture->changed, fixture->mutex)
                              : zb_cond_wait_until(fixture->changed,
                                                   fixture->mutex, deadline);
        if (context->status != ZEROBUS_STATUS_OK) {
            break;
        }
        fixture->wakeups++;
        context->status = zb_cond_broadcast(fixture->progress);
    }
    if (context->status == ZEROBUS_STATUS_OK && fixture->proceed &&
        !fixture->stop) {
        fixture->completed++;
    }
    zerobus_status_t status = zb_mutex_unlock(fixture->mutex);
    if (context->status == ZEROBUS_STATUS_OK) {
        context->status = status;
    }
    return NULL;
}

/* Start waiters and return with the mutex held once all are waiting. */
static bool start_waiters(struct wait_fixture *fixture, enum wait_mode mode,
                          unsigned int workers)
{
    REQUIRE_OK(zb_mutex_new(&fixture->mutex));
    REQUIRE_OK(zb_cond_new(&fixture->changed));
    REQUIRE_OK(zb_cond_new(&fixture->progress));
    for (unsigned int i = 0; i < workers; i++) {
        fixture->contexts[i] =
            (struct wait_context){fixture, mode, ZEROBUS_STATUS_UNKNOWN};
        REQUIRE_OK(zb_thread_new(wait_for_permission, &fixture->contexts[i],
                                 &fixture->threads[i]));
        fixture->created++;
    }

    REQUIRE_OK(zb_mutex_lock(fixture->mutex));
    fixture->holding = true;
    zb_deadline_t deadline = zb_deadline_after_ms(WAIT_TIMEOUT_MS);
    while (fixture->ready != workers) {
        REQUIRE_OK(
            zb_cond_wait_until(fixture->progress, fixture->mutex, deadline));
    }
    return true;

cleanup:
    return false;
}

/* Join before teardown so cleanup cannot supply a missing notification. */
static void join_waiters(struct wait_fixture *fixture)
{
    if (fixture->holding) {
        CHECK_OK(zb_mutex_unlock(fixture->mutex));
        fixture->holding = false;
    }
    for (unsigned int i = 0; i < fixture->created; i++) {
        join_owned(&fixture->threads[i]);
        CHECK_OK(fixture->contexts[i].status);
    }
    fixture->created = 0;
}

/* Stop unfinished workers before releasing any resources they can access. */
static void destroy_waiters(struct wait_fixture *fixture)
{
    if (fixture->created > 0) {
        if (!fixture->holding) {
            zerobus_status_t status = zb_mutex_lock(fixture->mutex);
            if (status != ZEROBUS_STATUS_OK) {
                fprintf(stderr, "Waiter cleanup lock failed (status %u).\n",
                        (unsigned int)status);
                abort();
            }
            fixture->holding = true;
        }
        fixture->stop = true;
        CHECK_OK(zb_cond_broadcast(fixture->changed));
    }
    join_waiters(fixture);
    CHECK_OK(zb_cond_free(fixture->progress));
    CHECK_OK(zb_cond_free(fixture->changed));
    CHECK_OK(zb_mutex_free(fixture->mutex));
}

/* Ignore spurious wakeups while waiting for a fixed timeout to expire. */
static zerobus_status_t wait_for_timeout(zb_cond_t *cond, zb_mutex_t *mutex,
                                         zb_deadline_t deadline)
{
    zerobus_status_t status;
    do {
        status = zb_cond_wait_until(cond, mutex, deadline);
    } while (status == ZEROBUS_STATUS_OK);
    return status;
}

/* ------------------------------- Tests -------------------------------- */

static void test_mutex_arguments(void)
{
    zb_mutex_t *mutex = NULL;
    bool acquired = true;

    CHECK_EQ_INT(zb_mutex_new(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_mutex_lock(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_mutex_unlock(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_mutex_try_lock(NULL, &acquired),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(acquired);
    CHECK_OK(zb_mutex_free(NULL));

    REQUIRE_OK(zb_mutex_new(&mutex));
    CHECK_EQ_INT(zb_mutex_try_lock(mutex, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);

cleanup:
    CHECK_OK(zb_mutex_free(mutex));
}

static void test_condition_arguments(void)
{
    zb_mutex_t *mutex = NULL;
    zb_cond_t *cond = NULL;
    bool holding = false;

    CHECK_EQ_INT(zb_cond_new(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_cond_wait(NULL, NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_cond_wait_until(NULL, NULL, ZB_DEADLINE_IMMEDIATE),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_cond_signal(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_cond_broadcast(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_OK(zb_cond_free(NULL));

    REQUIRE_OK(zb_mutex_new(&mutex));
    REQUIRE_OK(zb_cond_new(&cond));
    CHECK_EQ_INT(zb_cond_wait(cond, NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_cond_wait_until(cond, NULL, ZB_DEADLINE_IMMEDIATE),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);

    REQUIRE_OK(zb_mutex_lock(mutex));
    holding = true;
    CHECK_EQ_INT(zb_cond_wait(NULL, mutex), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_cond_wait_until(NULL, mutex, ZB_DEADLINE_IMMEDIATE),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    bool acquired = false;
    CHECK_OK(zb_mutex_try_lock(mutex, &acquired));
    CHECK(!acquired);

cleanup:
    if (holding) {
        CHECK_OK(zb_mutex_unlock(mutex));
    }
    CHECK_OK(zb_cond_free(cond));
    CHECK_OK(zb_mutex_free(mutex));
}

static void test_thread_arguments(void)
{
    zb_thread_t *thread = NULL;
    int value = 0;
    void *result = &value;

    CHECK_EQ_INT(zb_thread_new(set_value, NULL, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_thread_new(NULL, NULL, &thread),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(thread == NULL);
    CHECK_EQ_INT(zb_thread_join(NULL, NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_thread_join(NULL, &result),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(result == &value);

    REQUIRE_OK(zb_thread_new(set_value, &value, &thread));
    zb_thread_t *output = thread;
    CHECK_EQ_INT(zb_thread_new(NULL, NULL, &output),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(output == thread);

cleanup:
    join_owned(&thread);
}

static void test_millisecond_deadlines(void)
{
    CHECK(zb_deadline_after_ms(0) == ZB_DEADLINE_IMMEDIATE);
    CHECK(zb_deadline_after_ms(UINT64_MAX) == ZB_DEADLINE_INFINITE);

    zb_deadline_t short_deadline = zb_deadline_after_ms(1);
    zb_deadline_t later_deadline = zb_deadline_after_ms(1000);
    CHECK(short_deadline > ZB_DEADLINE_IMMEDIATE);
    CHECK(later_deadline > short_deadline);

    zb_deadline_t largest = zb_deadline_after_ms(UINT64_MAX - 1);
    CHECK(largest < ZB_DEADLINE_INFINITE);
    CHECK(largest > later_deadline);
    CHECK(largest == zb_deadline_after_ms(UINT64_MAX - 2));
}

static void test_second_deadlines(void)
{
    CHECK(zb_deadline_after_seconds(0) == ZB_DEADLINE_IMMEDIATE);
    CHECK(zb_deadline_after_seconds(UINT64_MAX) == ZB_DEADLINE_INFINITE);

    zb_deadline_t before = zb_deadline_after_ms(2000);
    zb_deadline_t seconds = zb_deadline_after_seconds(2);
    zb_deadline_t after = zb_deadline_after_ms(2000);
    CHECK(before <= seconds);
    CHECK(seconds <= after);
}

static void test_second_deadline_saturation(void)
{
    uint64_t conversion_limit = (UINT64_MAX - 1) / UINT64_C(1000);
    zb_deadline_t largest = zb_deadline_after_ms(UINT64_MAX - 1);

    CHECK(zb_deadline_after_seconds(conversion_limit - 1) == largest);
    CHECK(zb_deadline_after_seconds(conversion_limit) == largest);
    CHECK(zb_deadline_after_seconds(conversion_limit + 1) == largest);
    CHECK(zb_deadline_after_seconds(UINT64_MAX - 1) == largest);
}

static void test_mutex_try_lock(void)
{
    zb_mutex_t *mutex = NULL;
    bool acquired = false;
    bool holding = false;

    REQUIRE_OK(zb_mutex_new(&mutex));
    REQUIRE_OK(zb_mutex_try_lock(mutex, &acquired));
    CHECK(acquired);
    holding = acquired;
    if (!acquired) {
        goto cleanup;
    }

    /* A normal mutex must not be acquired recursively. */
    REQUIRE_OK(zb_mutex_try_lock(mutex, &acquired));
    CHECK(!acquired);
    REQUIRE_OK(zb_mutex_unlock(mutex));
    holding = false;
    REQUIRE_OK(zb_mutex_try_lock(mutex, &acquired));
    CHECK(acquired);
    holding = acquired;

cleanup:
    if (holding) {
        CHECK_OK(zb_mutex_unlock(mutex));
    }
    CHECK_OK(zb_mutex_free(mutex));
}

static void test_mutex_contention(void)
{
    zb_mutex_t *mutex = NULL;
    bool holding = false;

    REQUIRE_OK(zb_mutex_new(&mutex));
    REQUIRE_OK(zb_mutex_lock(mutex));
    holding = true;
    struct try_context result = try_from_thread(mutex);
    CHECK_OK(result.status);
    CHECK(!result.acquired);

cleanup:
    if (holding) {
        CHECK_OK(zb_mutex_unlock(mutex));
    }
    CHECK_OK(zb_mutex_free(mutex));
}

static void test_mutex_exclusion(void)
{
    struct counter counter = {0};
    struct counter_context contexts[WORKERS];
    zb_thread_t *threads[WORKERS] = {0};
    unsigned int created = 0;

    REQUIRE_OK(zb_mutex_new(&counter.mutex));
    for (unsigned int i = 0; i < WORKERS; i++) {
        contexts[i] =
            (struct counter_context){&counter, ZEROBUS_STATUS_UNKNOWN};
        REQUIRE_OK(zb_thread_new(increment_counter, &contexts[i], &threads[i]));
        created++;
    }

cleanup:
    for (unsigned int i = 0; i < created; i++) {
        join_owned(&threads[i]);
        CHECK_OK(contexts[i].status);
    }
    CHECK_EQ_INT(counter.value, created * INCREMENTS);
    CHECK_OK(zb_mutex_free(counter.mutex));
}

static void test_condition_timeout_and_mutex_reacquisition(void)
{
    zb_mutex_t *mutex = NULL;
    zb_cond_t *cond = NULL;
    bool holding = false;

    REQUIRE_OK(zb_mutex_new(&mutex));
    REQUIRE_OK(zb_cond_new(&cond));
    REQUIRE_OK(zb_mutex_lock(mutex));
    holding = true;

    CHECK_EQ_INT(wait_for_timeout(cond, mutex, ZB_DEADLINE_IMMEDIATE),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    zb_deadline_t deadline = zb_deadline_after_ms(20);
    CHECK_EQ_INT(wait_for_timeout(cond, mutex, deadline),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    /* A realtime wait would misinterpret this monotonic deadline as past. */
    CHECK(zb_deadline_after_ms(1) > deadline);
    CHECK_EQ_INT(wait_for_timeout(cond, mutex, deadline),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);

    struct try_context result = try_from_thread(mutex);
    CHECK_OK(result.status);
    CHECK(!result.acquired);

cleanup:
    if (holding) {
        CHECK_OK(zb_mutex_unlock(mutex));
    }
    CHECK_OK(zb_cond_free(cond));
    CHECK_OK(zb_mutex_free(mutex));
}

static void test_condition_signal(void)
{
    struct wait_fixture fixture = {0};
    if (!start_waiters(&fixture, WAIT_FINITE, 1)) {
        goto cleanup;
    }

    fixture.proceed = true;
    REQUIRE_OK(zb_cond_signal(fixture.changed));
    join_waiters(&fixture);
    CHECK_EQ_INT(fixture.completed, 1);

cleanup:
    destroy_waiters(&fixture);
}

static void test_condition_broadcast(void)
{
    struct wait_fixture fixture = {0};
    if (!start_waiters(&fixture, WAIT_UNTIMED, WORKERS)) {
        goto cleanup;
    }

    fixture.proceed = true;
    REQUIRE_OK(zb_cond_broadcast(fixture.changed));
    join_waiters(&fixture);
    CHECK_EQ_INT(fixture.completed, WORKERS);

cleanup:
    destroy_waiters(&fixture);
}

static void test_condition_signal_without_predicate_change(void)
{
    struct wait_fixture fixture = {0};
    if (!start_waiters(&fixture, WAIT_FINITE, 1)) {
        goto cleanup;
    }

    unsigned int previous_wakeups = fixture.wakeups;
    REQUIRE_OK(zb_cond_signal(fixture.changed));
    zb_deadline_t deadline = zb_deadline_after_ms(WAIT_TIMEOUT_MS);
    while (fixture.wakeups == previous_wakeups) {
        REQUIRE_OK(
            zb_cond_wait_until(fixture.progress, fixture.mutex, deadline));
    }
    CHECK(!fixture.proceed);
    CHECK_EQ_INT(fixture.completed, 0);

    fixture.proceed = true;
    REQUIRE_OK(zb_cond_signal(fixture.changed));
    join_waiters(&fixture);
    CHECK_EQ_INT(fixture.completed, 1);

cleanup:
    destroy_waiters(&fixture);
}

static void test_condition_infinite_deadline(void)
{
    struct wait_fixture fixture = {0};
    if (!start_waiters(&fixture, WAIT_INFINITE, 1)) {
        goto cleanup;
    }

    fixture.proceed = true;
    REQUIRE_OK(zb_cond_signal(fixture.changed));
    join_waiters(&fixture);
    CHECK_EQ_INT(fixture.completed, 1);

cleanup:
    destroy_waiters(&fixture);
}

static void test_condition_large_deadline(void)
{
    struct wait_fixture fixture = {0};
    if (!start_waiters(&fixture, WAIT_LARGE, 1)) {
        goto cleanup;
    }

    fixture.proceed = true;
    REQUIRE_OK(zb_cond_signal(fixture.changed));
    join_waiters(&fixture);
    CHECK_EQ_INT(fixture.completed, 1);

cleanup:
    destroy_waiters(&fixture);
}

static void test_thread_completion(void)
{
    zb_thread_t *thread = NULL;
    int value = 0;

    for (unsigned int i = 0; i < 32; i++) {
        value = 0;
        REQUIRE_OK(zb_thread_new(set_value, &value, &thread));
        join_owned(&thread);
        CHECK_EQ_INT(value, 42);
    }

cleanup:
    join_owned(&thread);
}

static void test_thread_returned_result(void)
{
    zb_thread_t *thread = NULL;
    int value = 0;
    void *result = NULL;

    REQUIRE_OK(zb_thread_new(set_value, &value, &thread));
    REQUIRE_OK(zb_thread_join(thread, &result));
    thread = NULL;
    CHECK(result == &value);
    CHECK_EQ_INT(value, 42);

cleanup:
    join_owned(&thread);
}

static void test_thread_null_result(void)
{
    zb_thread_t *thread = NULL;
    int sentinel = 0;
    void *result = &sentinel;

    REQUIRE_OK(zb_thread_new(set_value, NULL, &thread));
    REQUIRE_OK(zb_thread_join(thread, &result));
    thread = NULL;
    CHECK(result == NULL);

cleanup:
    join_owned(&thread);
}

static void test_thread_ignored_result(void)
{
    zb_thread_t *thread = NULL;
    int value = 0;

    REQUIRE_OK(zb_thread_new(set_value, &value, &thread));
    REQUIRE_OK(zb_thread_join(thread, NULL));
    thread = NULL;
    CHECK_EQ_INT(value, 42);

cleanup:
    join_owned(&thread);
}

int main(void)
{
    test_mutex_arguments();
    test_condition_arguments();
    test_thread_arguments();

    test_millisecond_deadlines();
    test_second_deadlines();
    test_second_deadline_saturation();

    test_mutex_try_lock();
    test_mutex_contention();
    test_mutex_exclusion();

    test_condition_timeout_and_mutex_reacquisition();
    test_condition_signal();
    test_condition_broadcast();
    test_condition_signal_without_predicate_change();
    test_condition_infinite_deadline();
    test_condition_large_deadline();

    test_thread_completion();
    test_thread_returned_result();
    test_thread_null_result();
    test_thread_ignored_result();
    TEST_MAIN_RETURN();
}
