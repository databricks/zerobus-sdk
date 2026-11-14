#include <stdlib.h>

#include "internal/landing_zone.h"
#include "internal/platform/concurrency.h"
#include "internal/proto/zerobus_service.h"
#include "test_common.h"

struct worker {
    zb_landing_zone_t *zone;
    zb_ingest_request_t *request;
    zerobus_status_t status;
    zerobus_offset_t offset;
    bool pushing, initialized, started;
    zb_sem_t ready;
    zb_thread_t thread;
};

static void must_succeed(zerobus_status_t status)
{
    if (status != ZEROBUS_STATUS_OK) {
        fprintf(stderr, "Test synchronization failed (status %u).\n",
                (unsigned int)status);
        abort();
    }
}

static zerobus_status_t push_request(zb_landing_zone_t *zone, char data,
                                     zerobus_offset_t *offset)
{
    zb_ingest_request_t *request =
        zb_ingest_request_new(&(zerobus_string_view_t){&data, 1}, 1);
    if (request == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    zerobus_status_t status =
        zb_landing_zone_push(zone, request, ZB_DEADLINE_IMMEDIATE, offset);
    if (status != ZEROBUS_STATUS_OK) {
        zb_ingest_request_free(request);
    }
    return status;
}

static void *run_worker(void *arg)
{
    struct worker *worker = (struct worker *)arg;
    /* Readiness precedes the call; it does not prove the worker is blocked. */
    must_succeed(zb_sem_post(&worker->ready));
    zb_deadline_t deadline = zb_deadline_after_ms(5000);
    worker->status =
        worker->pushing
            ? zb_landing_zone_push(worker->zone, worker->request, deadline,
                                   &worker->offset)
            : zb_landing_zone_observe(worker->zone, deadline, &worker->request,
                                      &worker->offset);
    return NULL;
}

/* Once called, finish_worker owns the resources and any rejected request. */
static zerobus_status_t start_worker(struct worker *worker,
                                     zb_landing_zone_t *zone)
{
    worker->zone = zone;
    worker->pushing = worker->request != NULL;
    worker->offset = -1;
    worker->status = ZEROBUS_STATUS_UNKNOWN;
    zerobus_status_t status = zb_sem_init(&worker->ready, 0);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    worker->initialized = true;
    status = zb_thread_create(&worker->thread, run_worker, worker);
    worker->started = status == ZEROBUS_STATUS_OK;
    return status;
}

static void join_worker(struct worker *worker)
{
    if (worker->started) {
        must_succeed(zb_thread_join(&worker->thread, NULL));
        worker->started = false;
    }
}

static void finish_worker(struct worker *worker)
{
    join_worker(worker);
    if (worker->pushing && worker->status != ZEROBUS_STATUS_OK) {
        zb_ingest_request_free(worker->request);
    }
    if (worker->initialized) {
        must_succeed(zb_sem_destroy(&worker->ready));
    }
    *worker = (struct worker){0};
}

/* ---- tests ------------------------------------------------------------- */

static void test_invalid_arguments(void)
{
    zb_landing_zone_t *zone = NULL;
    zb_ingest_request_t marker = {0}, *borrowed = &marker;
    zerobus_offset_t offset = 99;
    CHECK_EQ_INT(zb_landing_zone_new(4, NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    zb_landing_zone_t *unchanged = zone;
    CHECK_EQ_INT(zb_landing_zone_new(ZB_SEM_VALUE_MAX, &unchanged),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(unchanged == zone);
    CHECK_EQ_INT(zb_landing_zone_new(0, &unchanged),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(unchanged == zone);
    CHECK_EQ_INT(
        zb_landing_zone_push(NULL, &marker, ZB_DEADLINE_IMMEDIATE, &offset),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(
        zb_landing_zone_push(zone, NULL, ZB_DEADLINE_IMMEDIATE, &offset),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_landing_zone_observe(NULL, ZB_DEADLINE_IMMEDIATE, &borrowed,
                                         &offset),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(zb_landing_zone_pop(NULL, &offset) == NULL);
    CHECK_EQ_INT(zb_landing_zone_reset_observe(NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_landing_zone_close(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(zb_landing_zone_last_offset(NULL) == -1);
    zb_landing_zone_free(NULL);
    CHECK(borrowed == &marker);
    CHECK_EQ_INT(offset, 99);

zb_cleanup:
    zb_landing_zone_free(zone);
}

static void test_empty_waits(void)
{
    zb_landing_zone_t *zone = NULL;
    zb_ingest_request_t marker = {0}, *borrowed = &marker;
    zerobus_offset_t offset = 99;
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    CHECK_OK(zb_landing_zone_reset_observe(zone));
    CHECK(zb_landing_zone_pop(zone, &offset) == NULL);
    CHECK_EQ_INT(zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, &borrowed,
                                         &offset),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK_EQ_INT(zb_landing_zone_observe(zone, zb_deadline_after_ms(1),
                                         &borrowed, &offset),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK_EQ_INT(
        zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, NULL, &offset),
        ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK_EQ_INT(
        zb_landing_zone_observe(zone, zb_deadline_after_ms(1), NULL, NULL),
        ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK(borrowed == &marker);
    CHECK_EQ_INT(offset, 99);

zb_cleanup:
    zb_landing_zone_free(zone);
}

static void test_fifo_and_offsets(void)
{
    zb_landing_zone_t *zone = NULL;
    zb_ingest_request_t *borrowed = NULL, *owned = NULL;
    zerobus_offset_t offset = -1;
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    CHECK(zb_landing_zone_last_offset(zone) == -1);
    for (unsigned int i = 0; i < 4; i++) {
        REQUIRE_OK(push_request(zone, (char)('0' + i), &offset));
        CHECK_EQ_INT(offset, i);
        CHECK_EQ_INT(zb_landing_zone_last_offset(zone), i);
    }
    for (unsigned int i = 0; i < 4; i++) {
        REQUIRE_OK(zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE,
                                           &borrowed, &offset));
        CHECK_EQ_INT(borrowed->batch[0].len, 1);
        CHECK_EQ_INT(borrowed->batch[0].data[0], '0' + i);
        CHECK_EQ_INT(offset, i);
    }
    for (unsigned int i = 0; i < 4; i++) {
        owned = zb_landing_zone_pop(zone, &offset);
        REQUIRE(owned != NULL);
        CHECK_EQ_INT(owned->batch[0].data[0], '0' + i);
        CHECK_EQ_INT(offset, i);
        zb_ingest_request_free(owned);
        owned = NULL;
    }
    CHECK_EQ_INT(zb_landing_zone_last_offset(zone), 3);

zb_cleanup:
    zb_ingest_request_free(owned);
    zb_landing_zone_free(zone);
}

static void test_observe_with_optional_outputs(void)
{
    zb_landing_zone_t *zone = NULL;
    zb_ingest_request_t *owned = NULL;
    zerobus_offset_t offset = -1;
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    for (unsigned int i = 0; i < 3; i++) {
        REQUIRE_OK(push_request(zone, (char)('0' + i), NULL));
    }
    for (unsigned int i = 0; i < 2; i++) {
        REQUIRE_OK(zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, NULL,
                                           i == 0 ? &offset : NULL));
        if (i == 0) {
            CHECK_EQ_INT(offset, 0);
        }
        owned = zb_landing_zone_pop(zone, &offset);
        REQUIRE(owned != NULL);
        CHECK_EQ_INT(offset, i);
        CHECK_EQ_INT(owned->batch[0].len, 1);
        CHECK_EQ_INT(owned->batch[0].data[0], '0' + i);
        zb_ingest_request_free(owned);
        owned = NULL;
        zb_ingest_request_t *extra = zb_landing_zone_pop(zone, NULL);
        CHECK(extra == NULL);
        zb_ingest_request_free(extra);
    }

zb_cleanup:
    zb_ingest_request_free(owned);
    zb_landing_zone_free(zone);
}

static void test_full_waits_and_pop_releases_space(void)
{
    zb_landing_zone_t *zone = NULL;
    zb_ingest_request_t *pending = NULL, *borrowed = NULL, *owned = NULL;
    zerobus_offset_t offset = 99;
    /* A limit below the ring's power-of-two size still blocks. */
    REQUIRE_OK(zb_landing_zone_new(5, &zone));
    for (unsigned int i = 0; i < 5; i++) {
        REQUIRE_OK(push_request(zone, (char)('0' + i), NULL));
        REQUIRE_OK(zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE,
                                           &borrowed, NULL));
    }
    pending = zb_ingest_request_new(&(zerobus_string_view_t){"5", 1}, 1);
    REQUIRE(pending != NULL);
    CHECK_EQ_INT(
        zb_landing_zone_push(zone, pending, ZB_DEADLINE_IMMEDIATE, &offset),
        ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    zb_deadline_t deadline = zb_deadline_after_ms(1);
    CHECK_EQ_INT(zb_landing_zone_push(zone, pending, deadline, &offset),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK_EQ_INT(offset, 99);
    CHECK_EQ_INT(pending->batch[0].data[0], '5');
    owned = zb_landing_zone_pop(zone, &offset);
    REQUIRE(owned != NULL);
    CHECK_EQ_INT(offset, 0);
    REQUIRE_OK(zb_landing_zone_push(zone, pending, deadline, &offset));
    pending = NULL;
    CHECK_EQ_INT(offset, 5);
    REQUIRE_OK(zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, &borrowed,
                                       &offset));
    CHECK_EQ_INT(borrowed->batch[0].data[0], '5');
    CHECK_EQ_INT(offset, 5);

zb_cleanup:
    zb_ingest_request_free(owned);
    zb_ingest_request_free(pending);
    zb_landing_zone_free(zone);
}

static void test_reset_observe_replays_retained_requests(void)
{
    zb_landing_zone_t *zone = NULL;
    zb_ingest_request_t *original[3] = {NULL}, *borrowed = NULL, *owned = NULL;
    zerobus_offset_t offset = -1;
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    for (unsigned int i = 0; i < 4; i++) {
        REQUIRE_OK(push_request(zone, (char)('0' + i), NULL));
    }
    CHECK_OK(zb_landing_zone_reset_observe(zone));
    for (unsigned int i = 0; i < 3; i++) {
        REQUIRE_OK(zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE,
                                           &original[i], NULL));
    }
    owned = zb_landing_zone_pop(zone, NULL);
    REQUIRE(owned != NULL);
    CHECK(owned == original[0]);
    zb_ingest_request_free(owned);
    owned = NULL;

    CHECK_OK(zb_landing_zone_reset_observe(zone));
    CHECK_OK(zb_landing_zone_reset_observe(zone));
    CHECK_EQ_INT(zb_landing_zone_last_offset(zone), 3);
    owned = zb_landing_zone_pop(zone, NULL);
    REQUIRE(owned == NULL);
    REQUIRE_OK(push_request(zone, '4', &offset));
    CHECK_EQ_INT(offset, 4);
    CHECK_EQ_INT(zb_landing_zone_last_offset(zone), 4);
    CHECK_EQ_INT(push_request(zone, '5', NULL),
                 ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    for (unsigned int i = 1; i < 5; i++) {
        REQUIRE_OK(zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE,
                                           &borrowed, &offset));
        CHECK_EQ_INT(offset, i);
        CHECK_EQ_INT(borrowed->batch[0].data[0], '0' + i);
        if (i < 3) {
            CHECK(borrowed == original[i]);
        }
        owned = zb_landing_zone_pop(zone, NULL);
        REQUIRE(owned != NULL);
        CHECK(owned == borrowed);
        zb_ingest_request_free(owned);
        owned = NULL;
    }
    CHECK_EQ_INT(
        zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, NULL, NULL),
        ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK_EQ_INT(zb_landing_zone_last_offset(zone), 4);

zb_cleanup:
    zb_ingest_request_free(owned);
    zb_landing_zone_free(zone);
}

static void test_reset_observe_closed_zone(void)
{
    zb_landing_zone_t *zone = NULL;
    zb_ingest_request_t *borrowed = NULL, *owned = NULL;
    zerobus_offset_t offset = -1;
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    for (unsigned int i = 0; i < 4; i++) {
        REQUIRE_OK(push_request(zone, (char)('0' + i), NULL));
        REQUIRE_OK(
            zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, NULL, NULL));
    }
    CHECK_OK(zb_landing_zone_close(zone));
    CHECK_OK(zb_landing_zone_reset_observe(zone));
    CHECK_EQ_INT(push_request(zone, '4', NULL),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    for (unsigned int i = 0; i < 4; i++) {
        REQUIRE_OK(zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE,
                                           &borrowed, &offset));
        CHECK_EQ_INT(offset, i);
        CHECK_EQ_INT(borrowed->batch[0].data[0], '0' + i);
        owned = zb_landing_zone_pop(zone, NULL);
        REQUIRE(owned != NULL);
        CHECK(owned == borrowed);
        zb_ingest_request_free(owned);
        owned = NULL;
    }
    CHECK_EQ_INT(
        zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, NULL, NULL),
        ZEROBUS_STATUS_RESOURCE_EXHAUSTED);

zb_cleanup:
    zb_ingest_request_free(owned);
    zb_landing_zone_free(zone);
}

static void test_close_rejects_push_and_allows_observe_and_pop(void)
{
    zb_landing_zone_t *zone = NULL;
    zb_ingest_request_t marker = {0}, *borrowed = NULL, *owned = NULL;
    zerobus_offset_t offset = 99;
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    REQUIRE_OK(push_request(zone, '0', NULL));
    CHECK_OK(zb_landing_zone_close(zone));
    CHECK_OK(zb_landing_zone_close(zone));
    CHECK_EQ_INT(
        zb_landing_zone_push(zone, &marker, ZB_DEADLINE_IMMEDIATE, &offset),
        ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK_EQ_INT(offset, 99);
    CHECK_EQ_INT(zb_landing_zone_last_offset(zone), 0);
    REQUIRE_OK(zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, &borrowed,
                                       &offset));
    CHECK_EQ_INT(borrowed->batch[0].data[0], '0');
    CHECK_EQ_INT(offset, 0);
    CHECK_EQ_INT(zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, &borrowed,
                                         &offset),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK_EQ_INT(zb_landing_zone_observe(zone, zb_deadline_after_ms(1),
                                         &borrowed, &offset),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK_EQ_INT(borrowed->batch[0].data[0], '0');
    CHECK_EQ_INT(offset, 0);

    offset = 99;
    owned = zb_landing_zone_pop(zone, &offset);
    REQUIRE(owned != NULL);
    CHECK(owned == borrowed);
    CHECK_EQ_INT(offset, 0);

zb_cleanup:
    zb_ingest_request_free(owned);
    zb_landing_zone_free(zone);
}

static void test_pop_transfers_ownership(void)
{
    zb_landing_zone_t *zone = NULL;
    zb_ingest_request_t *borrowed = NULL, *owned = NULL;
    zerobus_offset_t offset = -1;
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    for (unsigned int i = 0; i < 3; i++) {
        REQUIRE_OK(push_request(zone, (char)('0' + i), NULL));
    }
    REQUIRE_OK(
        zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, &borrowed, NULL));
    owned = zb_landing_zone_pop(zone, &offset);
    REQUIRE(owned != NULL);
    CHECK(owned == borrowed);
    CHECK_EQ_INT(offset, 0);
    REQUIRE_OK(
        zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, &borrowed, NULL));
    /* Free the zone with both observed and unobserved leftovers. */
    zb_landing_zone_free(zone);
    zone = NULL;
    CHECK_EQ_INT(owned->batch[0].len, 1);
    CHECK_EQ_INT(owned->batch[0].data[0], '0');

zb_cleanup:
    zb_ingest_request_free(owned);
    zb_landing_zone_free(zone);
}

static void test_producer_progress_after_pop(void)
{
    zb_landing_zone_t *zone = NULL;
    zb_ingest_request_t *borrowed = NULL, *owned = NULL;
    struct worker producer = {0};
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    for (unsigned int i = 0; i < 4; i++) {
        REQUIRE_OK(push_request(zone, '0', NULL));
    }
    producer.request =
        zb_ingest_request_new(&(zerobus_string_view_t){"4", 1}, 1);
    REQUIRE(producer.request != NULL);
    REQUIRE_OK(start_worker(&producer, zone));
    REQUIRE_OK(zb_sem_wait(&producer.ready, zb_deadline_after_ms(5000)));
    REQUIRE_OK(
        zb_landing_zone_observe(zone, ZB_DEADLINE_IMMEDIATE, &borrowed, NULL));
    owned = zb_landing_zone_pop(zone, NULL);
    REQUIRE(owned != NULL);
    join_worker(&producer);
    CHECK_OK(producer.status);
    CHECK_EQ_INT(producer.offset, 4);

zb_cleanup:
    (void)zb_landing_zone_close(zone);
    finish_worker(&producer);
    zb_ingest_request_free(owned);
    zb_landing_zone_free(zone);
}

static void test_observer_progress_after_push(void)
{
    zb_landing_zone_t *zone = NULL;
    struct worker observer = {0};
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    REQUIRE_OK(start_worker(&observer, zone));
    REQUIRE_OK(zb_sem_wait(&observer.ready, zb_deadline_after_ms(5000)));
    REQUIRE_OK(push_request(zone, '0', NULL));
    join_worker(&observer);
    CHECK_OK(observer.status);
    REQUIRE(observer.request != NULL);
    CHECK_EQ_INT(observer.request->batch[0].data[0], '0');
    CHECK_EQ_INT(observer.offset, 0);

zb_cleanup:
    finish_worker(&observer);
    zb_landing_zone_free(zone);
}

static void test_close_rejects_producers(void)
{
    zb_landing_zone_t *zone = NULL;
    struct worker producers[4] = {0};
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    for (unsigned int i = 0; i < 4; i++) {
        REQUIRE_OK(push_request(zone, '0', NULL));
    }
    for (unsigned int i = 0; i < 4; i++) {
        producers[i].request =
            zb_ingest_request_new(&(zerobus_string_view_t){"4", 1}, 1);
        REQUIRE(producers[i].request != NULL);
        REQUIRE_OK(start_worker(&producers[i], zone));
        REQUIRE_OK(
            zb_sem_wait(&producers[i].ready, zb_deadline_after_ms(5000)));
    }
    CHECK_OK(zb_landing_zone_close(zone));
    CHECK_EQ_INT(zb_landing_zone_last_offset(zone), 3);
    for (unsigned int i = 0; i < 4; i++) {
        join_worker(&producers[i]);
        CHECK_EQ_INT(producers[i].status, ZEROBUS_STATUS_FAILED_PRECONDITION);
        CHECK_EQ_INT(producers[i].offset, -1);
        CHECK_EQ_INT(producers[i].request->batch[0].data[0], '4');
    }

zb_cleanup:
    (void)zb_landing_zone_close(zone);
    for (unsigned int i = 0; i < 4; i++) {
        finish_worker(&producers[i]);
    }
    zb_landing_zone_free(zone);
}

static void test_close_wakes_observer(void)
{
    zb_landing_zone_t *zone = NULL;
    struct worker observer = {0};
    REQUIRE_OK(zb_landing_zone_new(4, &zone));
    REQUIRE_OK(start_worker(&observer, zone));
    REQUIRE_OK(zb_sem_wait(&observer.ready, zb_deadline_after_ms(5000)));
    CHECK_OK(zb_landing_zone_close(zone));
    join_worker(&observer);
    CHECK_EQ_INT(observer.status, ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK(observer.request == NULL);
    CHECK(observer.offset == -1);

zb_cleanup:
    finish_worker(&observer);
    zb_landing_zone_free(zone);
}

int main(void)
{
    test_invalid_arguments();
    test_empty_waits();
    test_fifo_and_offsets();
    test_observe_with_optional_outputs();
    test_full_waits_and_pop_releases_space();
    test_reset_observe_replays_retained_requests();
    test_reset_observe_closed_zone();
    test_close_rejects_push_and_allows_observe_and_pop();
    test_pop_transfers_ownership();
    test_producer_progress_after_pop();
    test_observer_progress_after_push();
    test_close_rejects_producers();
    test_close_wakes_observer();
    TEST_MAIN_RETURN();
}
