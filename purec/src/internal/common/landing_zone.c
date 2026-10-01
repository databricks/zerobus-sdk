#define _POSIX_C_SOURCE 200809L

#include <stdatomic.h>
#include <stdlib.h>

#include "internal/log.h"
#include "landing_zone.h"
#include "queue.h"

struct zb_landing_zone {
    zb_queue_t *queue;
    zb_sem_t space;
    zb_sem_t items;
    atomic_bool closed;
};

zerobus_status_t zb_landing_zone_new(size_t capacity,
                                     zb_landing_zone_t **out_zone)
{
    /* The space semaphore needs room for one extra close token. */
    if (out_zone == NULL || capacity >= ZB_SEM_VALUE_MAX) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zb_landing_zone_t *zone = (zb_landing_zone_t *)calloc(1, sizeof(*zone));
    if (zone == NULL) {
        ZB_ERROR("landing zone allocation failed");
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    atomic_init(&zone->closed, false);

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

zerobus_status_t zb_landing_zone_push(zb_landing_zone_t *zone,
                                      zb_record_t *record,
                                      zb_deadline_t deadline,
                                      zerobus_offset_t *out_offset)
{
    if (zone == NULL || record == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (atomic_load(&zone->closed)) {
        return ZEROBUS_STATUS_FAILED_PRECONDITION;
    }
    zerobus_status_t status = zb_sem_wait(&zone->space, deadline);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    if (atomic_load(&zone->closed)) {
        (void)zb_sem_post(&zone->space);
        return ZEROBUS_STATUS_FAILED_PRECONDITION;
    }
    size_t position;
    while ((status = zb_queue_push(zone->queue, record, &position)) ==
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

zerobus_status_t zb_landing_zone_observe(zb_landing_zone_t *zone,
                                         zb_deadline_t deadline,
                                         zb_record_t **out_record,
                                         zerobus_offset_t *out_offset)
{
    if (zone == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    zerobus_status_t status = zb_sem_wait(&zone->items, deadline);
    if (status != ZEROBUS_STATUS_OK) {
        return status;
    }
    size_t position;
    zb_record_t *record;
    while ((record = (zb_record_t *)zb_queue_advance(zone->queue, &position)) ==
           NULL) {
        zb_thread_yield();
    }
    if (out_record != NULL) {
        *out_record = record;
    }
    if (out_offset != NULL) {
        *out_offset = (zerobus_offset_t)position;
    }
    return ZEROBUS_STATUS_OK;
}

zb_record_t *zb_landing_zone_pop(zb_landing_zone_t *zone,
                                 zerobus_offset_t *out_offset)
{
    if (zone == NULL) {
        return NULL;
    }
    size_t position;
    zb_record_t *record = (zb_record_t *)zb_queue_pop(zone->queue, &position);
    if (record != NULL) {
        (void)zb_sem_post(&zone->space);
        if (out_offset != NULL) {
            *out_offset = (zerobus_offset_t)position;
        }
    }
    return record;
}

zerobus_status_t zb_landing_zone_close(zb_landing_zone_t *zone)
{
    if (zone == NULL) {
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    if (!atomic_exchange(&zone->closed, true)) {
        /* Wake waiting producers. */
        (void)zb_sem_post(&zone->space);
    }
    return ZEROBUS_STATUS_OK;
}

void zb_landing_zone_free(zb_landing_zone_t *zone)
{
    if (zone == NULL) {
        return;
    }
    while (zb_queue_advance(zone->queue, NULL) != NULL) {
    }
    zb_record_t *record;
    while ((record = (zb_record_t *)zb_queue_pop(zone->queue, NULL)) != NULL) {
        zb_record_free(record);
    }
    zb_queue_free(zone->queue);
    (void)zb_sem_destroy(&zone->items);
    (void)zb_sem_destroy(&zone->space);
    free(zone);
}
