/* Unit tests for the bounded MPMC queue (queue.c). */
/* Keep sched_yield declared without changing the project's C baseline. */
#define _POSIX_C_SOURCE 200809L

#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>

#include "internal/common/queue.h"
#include "internal/concurrency.h"
#include "test_common.h"

enum {
    WRAPPED_ITEMS = 32,
    PRODUCERS = 2,
    ADVANCERS = 2,
    CONSUMERS = 2,
    WORKERS = PRODUCERS + ADVANCERS + CONSUMERS,
    ITEMS_PER_PRODUCER = 2000,
    STRESS_ITEMS = PRODUCERS * ITEMS_PER_PRODUCER
};

/* ---- helpers ----------------------------------------------------------- */

/* Shared by the stress workers. values[i] == i, and producer p pushes
 * &values[p * ITEMS_PER_PRODUCER + k] for k = 0, 1, ... in order, recording
 * the position each push returned in positions[value]. */
struct stress {
    zb_queue_t *queue;
    unsigned int values[STRESS_ITEMS];
    size_t positions[STRESS_ITEMS];
    atomic_uint advanced;
    atomic_uint consumed;
    atomic_bool stop; /* set when the test bails out early */
};

enum role { ROLE_PUSH, ROLE_ADVANCE, ROLE_POP };

/* One stress worker. Advancers and poppers record what they got, in order,
 * with the position the queue reported for it. */
struct worker {
    struct stress *stress;
    enum role role;
    unsigned int producer;
    zerobus_status_t status;
    unsigned int count;
    unsigned int got[STRESS_ITEMS];
    size_t got_positions[STRESS_ITEMS];
};

/* Spin on a full or empty queue: the queue never blocks. */
static void *run_worker(void *arg)
{
    struct worker *worker = (struct worker *)arg;
    struct stress *stress = worker->stress;
    if (worker->role == ROLE_PUSH) {
        unsigned int first = worker->producer * ITEMS_PER_PRODUCER;
        unsigned int pushed = 0;
        while (pushed < ITEMS_PER_PRODUCER && !atomic_load(&stress->stop)) {
            unsigned int value = first + pushed;
            zerobus_status_t status =
                zb_queue_push(stress->queue, &stress->values[value],
                              &stress->positions[value]);
            if (status == ZEROBUS_STATUS_OK) {
                pushed++;
            } else if (status == ZEROBUS_STATUS_RESOURCE_EXHAUSTED) {
                (void)sched_yield();
            } else {
                worker->status = status;
                return NULL;
            }
        }
        return NULL;
    }
    atomic_uint *done =
        worker->role == ROLE_ADVANCE ? &stress->advanced : &stress->consumed;
    while (atomic_load(done) < STRESS_ITEMS && !atomic_load(&stress->stop)) {
        size_t position;
        void *item = worker->role == ROLE_ADVANCE
                         ? zb_queue_advance(stress->queue, &position)
                         : zb_queue_pop(stress->queue, &position);
        if (item == NULL) {
            (void)sched_yield();
            continue;
        }
        worker->got[worker->count] = *(const unsigned int *)item;
        worker->got_positions[worker->count] = position;
        worker->count++;
        atomic_fetch_add(done, 1);
    }
    return NULL;
}

/* A failed join must not let a worker outlive the state it borrows. */
static void join_all(zb_thread_t *threads, unsigned int *running)
{
    for (unsigned int i = 0; i < *running; i++) {
        if (zb_thread_join(&threads[i], NULL) != ZEROBUS_STATUS_OK) {
            fputs("Thread cleanup join failed.\n", stderr);
            abort();
        }
    }
    *running = 0;
}

/* ---- tests ------------------------------------------------------------- */

static void test_new_rejects_invalid_arguments(void)
{
    int sentinel = 0;
    zb_queue_t *queue = (zb_queue_t *)(void *)&sentinel;
    CHECK_EQ_INT(zb_queue_new(0, &queue), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_queue_new(2, &queue), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_queue_new(6, &queue), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_queue_new((SIZE_MAX >> 1) + 1, &queue),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_queue_new(4, NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(queue == (zb_queue_t *)(void *)&sentinel);
    zb_queue_free(NULL);
}

static void test_null_queue_rejected(void)
{
    int value = 0;
    size_t position = 99;
    /* push reports the bad argument; advance, peek and pop can only signal it
     * by returning NULL. None writes out_position. */
    CHECK_EQ_INT(zb_queue_push(NULL, &value, &position),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(zb_queue_advance(NULL, &position) == NULL);
    CHECK(zb_queue_peek(NULL, &position) == NULL);
    CHECK(zb_queue_pop(NULL, &position) == NULL);
    CHECK_EQ_INT(position, 99);
}

static void test_empty_queue_returns_null(void)
{
    zb_queue_t *queue = NULL;
    int value = 0;
    size_t position = 99;
    REQUIRE_OK(zb_queue_new(4, &queue));
    CHECK(zb_queue_advance(queue, &position) == NULL);
    CHECK(zb_queue_peek(queue, &position) == NULL);
    CHECK(zb_queue_pop(queue, &position) == NULL);
    /* Returning no item leaves the position unchanged. */
    CHECK_EQ_INT(position, 99);

    /* An item not advanced yet cannot be peeked at or popped. */
    CHECK_OK(zb_queue_push(queue, &value, NULL));
    CHECK(zb_queue_peek(queue, &position) == NULL);
    CHECK(zb_queue_pop(queue, &position) == NULL);
    CHECK_EQ_INT(position, 99);
    CHECK(zb_queue_advance(queue, &position) == &value);
    CHECK_EQ_INT(position, 0);

zb_cleanup:
    zb_queue_free(queue);
}

static void test_push_rejects_null_item(void)
{
    zb_queue_t *queue = NULL;
    int value = 0;
    size_t position = 99;
    REQUIRE_OK(zb_queue_new(4, &queue));
    CHECK_EQ_INT(zb_queue_push(queue, NULL, &position),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(position, 99);
    CHECK(zb_queue_advance(queue, NULL) == NULL);

    /* The rejected push took no position. */
    CHECK_OK(zb_queue_push(queue, &value, &position));
    CHECK_EQ_INT(position, 0);

zb_cleanup:
    zb_queue_free(queue);
}

static void test_positions_are_optional(void)
{
    zb_queue_t *queue = NULL;
    int values[2] = {0, 1};
    size_t position = 99;
    REQUIRE_OK(zb_queue_new(4, &queue));

    /* A push without an out_position still takes its position. */
    CHECK_OK(zb_queue_push(queue, &values[0], NULL));
    CHECK_OK(zb_queue_push(queue, &values[1], &position));
    CHECK_EQ_INT(position, 1);

    CHECK(zb_queue_advance(queue, NULL) == &values[0]);
    CHECK(zb_queue_peek(queue, NULL) == &values[0]);
    CHECK(zb_queue_pop(queue, NULL) == &values[0]);

    position = 99;
    CHECK(zb_queue_advance(queue, &position) == &values[1]);
    CHECK_EQ_INT(position, 1);

zb_cleanup:
    zb_queue_free(queue);
}

static void test_advance_follows_push_order(void)
{
    zb_queue_t *queue = NULL;
    int values[3] = {0, 1, 2};
    size_t position;
    REQUIRE_OK(zb_queue_new(4, &queue));
    for (unsigned int i = 0; i < 3; i++) {
        CHECK_OK(zb_queue_push(queue, &values[i], &position));
        CHECK_EQ_INT(position, i);
    }
    for (unsigned int i = 0; i < 3; i++) {
        CHECK(zb_queue_advance(queue, &position) == &values[i]);
        CHECK_EQ_INT(position, i);
    }
    CHECK(zb_queue_advance(queue, NULL) == NULL);

    /* Advancing popped nothing: the oldest item is still held. */
    CHECK(zb_queue_peek(queue, &position) == &values[0]);
    CHECK_EQ_INT(position, 0);

zb_cleanup:
    zb_queue_free(queue);
}

static void test_pop_follows_advance_order(void)
{
    zb_queue_t *queue = NULL;
    int values[3] = {0, 1, 2};
    size_t position;
    REQUIRE_OK(zb_queue_new(4, &queue));
    for (unsigned int i = 0; i < 3; i++) {
        CHECK_OK(zb_queue_push(queue, &values[i], NULL));
    }
    CHECK(zb_queue_advance(queue, NULL) == &values[0]);
    CHECK(zb_queue_advance(queue, NULL) == &values[1]);

    CHECK(zb_queue_pop(queue, &position) == &values[0]);
    CHECK_EQ_INT(position, 0);
    CHECK(zb_queue_peek(queue, &position) == &values[1]);
    CHECK_EQ_INT(position, 1);
    CHECK(zb_queue_pop(queue, &position) == &values[1]);
    CHECK_EQ_INT(position, 1);
    /* values[2] is held but not advanced yet. */
    CHECK(zb_queue_pop(queue, NULL) == NULL);

    CHECK(zb_queue_advance(queue, NULL) == &values[2]);
    CHECK(zb_queue_pop(queue, &position) == &values[2]);
    CHECK_EQ_INT(position, 2);
    CHECK(zb_queue_advance(queue, NULL) == NULL);

zb_cleanup:
    zb_queue_free(queue);
}

static void test_full_until_pop(void)
{
    zb_queue_t *queue = NULL;
    int values[5] = {0, 1, 2, 3, 4};
    size_t position;
    REQUIRE_OK(zb_queue_new(4, &queue));
    for (unsigned int i = 0; i < 4; i++) {
        CHECK_OK(zb_queue_push(queue, &values[i], &position));
    }
    CHECK_EQ_INT(zb_queue_push(queue, &values[4], &position),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK_EQ_INT(position, 3);

    /* Advanced items still occupy their slots. */
    for (unsigned int i = 0; i < 4; i++) {
        CHECK(zb_queue_advance(queue, NULL) == &values[i]);
    }
    CHECK_EQ_INT(zb_queue_push(queue, &values[4], &position),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);

    /* The freed slot is reused, but positions keep counting. */
    CHECK(zb_queue_pop(queue, NULL) == &values[0]);
    CHECK_OK(zb_queue_push(queue, &values[4], &position));
    CHECK_EQ_INT(position, 4);
    CHECK(zb_queue_advance(queue, &position) == &values[4]);
    CHECK_EQ_INT(position, 4);

zb_cleanup:
    zb_queue_free(queue);
}

static void test_wraparound_preserves_order(void)
{
    zb_queue_t *queue = NULL;
    int values[WRAPPED_ITEMS] = {0};
    size_t position;
    REQUIRE_OK(zb_queue_new(4, &queue));

    /* Keep two items held so every counter wraps the ring many times. */
    unsigned int pushed = 0;
    unsigned int popped = 0;
    for (; pushed < 2; pushed++) {
        CHECK_OK(zb_queue_push(queue, &values[pushed], &position));
        CHECK_EQ_INT(position, pushed);
    }
    for (; pushed < WRAPPED_ITEMS; pushed++) {
        CHECK_OK(zb_queue_push(queue, &values[pushed], &position));
        CHECK_EQ_INT(position, pushed);
        CHECK(zb_queue_advance(queue, &position) == &values[popped]);
        CHECK_EQ_INT(position, popped);
        CHECK(zb_queue_pop(queue, &position) == &values[popped]);
        CHECK_EQ_INT(position, popped);
        popped++;
    }
    for (; popped < WRAPPED_ITEMS; popped++) {
        CHECK(zb_queue_advance(queue, &position) == &values[popped]);
        CHECK_EQ_INT(position, popped);
        CHECK(zb_queue_pop(queue, &position) == &values[popped]);
        CHECK_EQ_INT(position, popped);
    }
    CHECK(zb_queue_advance(queue, NULL) == NULL);
    CHECK(zb_queue_peek(queue, NULL) == NULL);

zb_cleanup:
    zb_queue_free(queue);
}

static void test_drain_returns_every_item(void)
{
    zb_queue_t *queue = NULL;
    int values[4] = {0, 1, 2, 3};
    size_t position;
    REQUIRE_OK(zb_queue_new(4, &queue));
    for (unsigned int i = 0; i < 4; i++) {
        CHECK_OK(zb_queue_push(queue, &values[i], NULL));
    }
    CHECK(zb_queue_advance(queue, NULL) == &values[0]);

    /* Advance until NULL, then pop until NULL: every item comes back, in push
     * order, whether it was advanced already or not. */
    while (zb_queue_advance(queue, NULL) != NULL) {
    }
    for (unsigned int i = 0; i < 4; i++) {
        CHECK(zb_queue_pop(queue, &position) == &values[i]);
        CHECK_EQ_INT(position, i);
    }
    CHECK(zb_queue_pop(queue, NULL) == NULL);

zb_cleanup:
    zb_queue_free(queue);
}

static void test_free_leaves_items_to_caller(void)
{
    zb_queue_t *queue = NULL;
    int *items[3] = {NULL, NULL, NULL};
    REQUIRE_OK(zb_queue_new(4, &queue));
    for (unsigned int i = 0; i < 3; i++) {
        items[i] = (int *)malloc(sizeof(*items[i]));
        REQUIRE(items[i] != NULL);
        *items[i] = (int)i;
        REQUIRE_OK(zb_queue_push(queue, items[i], NULL));
    }
    CHECK(zb_queue_advance(queue, NULL) == items[0]);

    /* Freeing with one advanced and two pushed items touches none of them:
     * they are still intact, and the test frees them below. ASan reports a
     * use after free or double free if the queue freed any, LSan a leak if
     * the test did not. */
    zb_queue_free(queue);
    queue = NULL;
    for (unsigned int i = 0; i < 3; i++) {
        CHECK_EQ_INT(*items[i], i);
    }

zb_cleanup:
    zb_queue_free(queue);
    for (unsigned int i = 0; i < 3; i++) {
        free(items[i]);
    }
}

/* Every stage runs on several threads at once over a tiny ring. */
static void test_concurrent_stages_keep_push_order(void)
{
    struct stress *stress = NULL;
    struct worker *workers = NULL;
    unsigned int *advanced = NULL; /* times each value was advanced */
    unsigned int *popped = NULL;   /* times each value was popped */
    unsigned int *claimed = NULL;  /* times each position was returned */
    zb_thread_t threads[WORKERS];
    unsigned int running = 0;

    stress = (struct stress *)calloc(1, sizeof(*stress));
    REQUIRE(stress != NULL);
    atomic_init(&stress->advanced, 0);
    atomic_init(&stress->consumed, 0);
    atomic_init(&stress->stop, false);
    workers = (struct worker *)calloc(WORKERS, sizeof(*workers));
    advanced = (unsigned int *)calloc(STRESS_ITEMS, sizeof(*advanced));
    popped = (unsigned int *)calloc(STRESS_ITEMS, sizeof(*popped));
    claimed = (unsigned int *)calloc(STRESS_ITEMS, sizeof(*claimed));
    REQUIRE(workers != NULL && advanced != NULL && popped != NULL &&
            claimed != NULL);
    REQUIRE_OK(zb_queue_new(4, &stress->queue));
    for (unsigned int i = 0; i < STRESS_ITEMS; i++) {
        stress->values[i] = i;
    }

    for (unsigned int i = 0; i < WORKERS; i++) {
        workers[i].stress = stress;
        workers[i].role = i < PRODUCERS               ? ROLE_PUSH
                          : i < PRODUCERS + ADVANCERS ? ROLE_ADVANCE
                                                      : ROLE_POP;
        workers[i].producer = i;
        REQUIRE_OK(zb_thread_create(&threads[i], run_worker, &workers[i]));
        running++;
    }
    join_all(threads, &running);

    /* Positions are 0..STRESS_ITEMS-1, each returned once, and increase in
     * each producer's push order. */
    for (unsigned int value = 0; value < STRESS_ITEMS; value++) {
        size_t position = stress->positions[value];
        REQUIRE(position < STRESS_ITEMS);
        claimed[position]++;
        if (value % ITEMS_PER_PRODUCER != 0) {
            CHECK(position > stress->positions[value - 1]);
        }
    }
    for (unsigned int i = 0; i < WORKERS; i++) {
        CHECK_OK(workers[i].status);
        if (workers[i].role == ROLE_PUSH) {
            continue;
        }
        unsigned int *counts =
            workers[i].role == ROLE_ADVANCE ? advanced : popped;
        long long last = -1;
        for (unsigned int k = 0; k < workers[i].count; k++) {
            unsigned int value = workers[i].got[k];
            size_t position = workers[i].got_positions[k];
            REQUIRE(value < STRESS_ITEMS);
            counts[value]++;
            /* Advance and pop report the position push returned. */
            CHECK_EQ_INT(position, stress->positions[value]);
            /* Any one consumer gets items in increasing position order, so
             * also in each producer's push order. */
            CHECK((long long)position > last);
            last = (long long)position;
        }
    }
    for (unsigned int i = 0; i < STRESS_ITEMS; i++) {
        CHECK_EQ_INT(claimed[i], 1);
        CHECK_EQ_INT(advanced[i], 1);
        CHECK_EQ_INT(popped[i], 1);
    }
    CHECK(zb_queue_advance(stress->queue, NULL) == NULL);
    CHECK(zb_queue_pop(stress->queue, NULL) == NULL);

zb_cleanup:
    if (stress != NULL) {
        atomic_store(&stress->stop, true);
    }
    join_all(threads, &running);
    if (stress != NULL) {
        zb_queue_free(stress->queue);
    }
    free(claimed);
    free(popped);
    free(advanced);
    free(workers);
    free(stress);
}

int main(void)
{
    test_new_rejects_invalid_arguments();
    test_null_queue_rejected();
    test_empty_queue_returns_null();
    test_push_rejects_null_item();
    test_positions_are_optional();
    test_advance_follows_push_order();
    test_pop_follows_advance_order();
    test_full_until_pop();
    test_wraparound_preserves_order();
    test_drain_returns_every_item();
    test_free_leaves_items_to_caller();
    test_concurrent_stages_keep_push_order();
    TEST_MAIN_RETURN();
}
