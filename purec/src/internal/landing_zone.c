#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "internal/common/queue.h"
#include "internal/platform/log.h"
#include "landing_zone.h"
#include "zerobus/common.h"

struct zb_landing_zone {
    zb_queue_t *queue;
    zb_sem_t space;
    zb_sem_t items;
    atomic_bool half_closed; /* new pushes are rejected */
    atomic_bool closed;      /* half_closed, and every admitted push landed */
    atomic_size_t in_flight; /* pushes that passed the first admission check */
};

zerobus_status_t zb_landing_zone_new(size_t capacity,
                                     zb_landing_zone_t **out_zone)
{
    /* Each semaphore needs room for one extra close token. */
    if (out_zone == NULL || capacity >= ZB_SEM_VALUE_MAX) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zb_landing_zone_t *zone = (zb_landing_zone_t *)calloc(1, sizeof(*zone));
    if (zone == NULL) {
        ZB_ERROR("landing zone allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    atomic_init(&zone->half_closed, false);
    atomic_init(&zone->closed, false);
    atomic_init(&zone->in_flight, 0);

    zerobus_status_t status = zb_queue_new(capacity, &zone->queue);
    if (status != ZEROBUS_STATUS_OK) {
        goto free_zone;
    }
    status = zb_sem_init(&zone->space, (unsigned int)capacity);
    if (status != ZEROBUS_STATUS_OK) {
        goto free_queue;
    }
    status = zb_sem_init(&zone->items, 0);
    if (status != ZEROBUS_STATUS_OK) {
        goto destroy_space;
    }
    *out_zone = zone;
    return ZEROBUS_STATUS_OK;

destroy_space:
    (void)zb_sem_destroy(&zone->space);
free_queue:
    zb_queue_free(zone->queue);
free_zone:
    free(zone);
    return status;
}

static zerobus_status_t push_counted(zb_landing_zone_t *zone,
                                     zb_ingest_request_t *request,
                                     zb_deadline_t deadline,
                                     zerobus_offset_t *out_offset)
{
    zerobus_status_t status = zb_sem_wait(&zone->space, deadline);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (atomic_load(&zone->half_closed)) {
        (void)zb_sem_post(&zone->space);
        return ZEROBUS_STATUS_FAILED_PRECONDITION;
    }
    uint64_t position;
    while ((status = zb_queue_push(zone->queue, request, &position)) ==
           ZEROBUS_STATUS_RESOURCE_EXHAUSTED) {
        zb_thread_yield();
    }
    if (status != ZEROBUS_STATUS_OK) {
        (void)zb_sem_post(&zone->space);
        return status;
    }
    (void)zb_sem_post(&zone->items);
    if (out_offset != NULL) {
        *out_offset = (zerobus_offset_t)position;
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_landing_zone_push(zb_landing_zone_t *zone,
                                      zb_ingest_request_t *request,
                                      zb_deadline_t deadline,
                                      zerobus_offset_t *out_offset)
{
    if (zone == NULL || request == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (atomic_load(&zone->half_closed)) {
        return ZEROBUS_STATUS_FAILED_PRECONDITION;
    }
    atomic_fetch_add(&zone->in_flight, 1);
    zerobus_status_t status = push_counted(zone, request, deadline, out_offset);
    atomic_fetch_sub(&zone->in_flight, 1);
    return status;
}

zerobus_status_t zb_landing_zone_observe(zb_landing_zone_t *zone,
                                         zb_deadline_t deadline,
                                         zb_ingest_request_t **out_request,
                                         zerobus_offset_t *out_offset)
{
    if (zone == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }

    bool closed = atomic_load(&zone->closed);
    zerobus_status_t status =
        zb_sem_wait(&zone->items, closed ? ZB_DEADLINE_IMMEDIATE : deadline);
    if (status != ZEROBUS_STATUS_OK) {
        return closed && status == ZEROBUS_STATUS_DEADLINE_EXCEEDED
                   ? ZEROBUS_STATUS_RESOURCE_EXHAUSTED
                   : status;
    }

    uint64_t position;
    zb_ingest_request_t *request;
    for (;;) {
        closed = atomic_load(&zone->closed);
        request =
            (zb_ingest_request_t *)zb_queue_advance(zone->queue, &position);
        if (closed || request != NULL) {
            break;
        }
        zb_thread_yield();
    }
    if (request == NULL) {
        (void)zb_sem_post(&zone->items);
        return ZEROBUS_STATUS_RESOURCE_EXHAUSTED;
    }

    if (out_request != NULL) {
        *out_request = request;
    }
    if (out_offset != NULL) {
        *out_offset = (zerobus_offset_t)position;
    }
    return ZEROBUS_STATUS_OK;
}

zb_ingest_request_t *zb_landing_zone_pop(zb_landing_zone_t *zone,
                                         zerobus_offset_t *out_offset)
{
    if (zone == NULL) {
        return NULL;
    }
    uint64_t position;
    zb_ingest_request_t *request =
        (zb_ingest_request_t *)zb_queue_pop(zone->queue, &position);
    if (request != NULL) {
        (void)zb_sem_post(&zone->space);
        if (out_offset != NULL) {
            *out_offset = (zerobus_offset_t)position;
        }
    }
    return request;
}

zerobus_status_t zb_landing_zone_reset_observe(zb_landing_zone_t *zone)
{
    if (zone == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    size_t count = zb_queue_rewind(zone->queue);
    for (size_t i = 0; i < count; i++) {
        (void)zb_sem_post(&zone->items);
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zb_landing_zone_close(zb_landing_zone_t *zone)
{
    if (zone == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (!atomic_exchange(&zone->half_closed, true)) {
        (void)zb_sem_post(&zone->space); /* wake waiting producers */
    }
    while (atomic_load(&zone->in_flight) != 0) {
        zb_thread_yield();
    }
    if (!atomic_exchange(&zone->closed, true)) {
        (void)zb_sem_post(&zone->items); /* wake waiting observers */
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_offset_t zb_landing_zone_last_offset(const zb_landing_zone_t *zone)
{
    if (zone == NULL) {
        return -1;
    }
    return (zerobus_offset_t)zb_queue_next_position(zone->queue) - 1;
}

void zb_landing_zone_free(zb_landing_zone_t *zone)
{
    if (zone == NULL) {
        return;
    }
    while (zb_queue_advance(zone->queue, NULL) != NULL) {
    }
    zb_ingest_request_t *request;
    while ((request = (zb_ingest_request_t *)zb_queue_pop(zone->queue, NULL)) !=
           NULL) {
        zb_ingest_request_free(request);
    }
    zb_queue_free(zone->queue);
    (void)zb_sem_destroy(&zone->items);
    (void)zb_sem_destroy(&zone->space);
    free(zone);
}
