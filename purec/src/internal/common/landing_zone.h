#ifndef ZB_LANDING_ZONE_H
#define ZB_LANDING_ZONE_H

#include <stddef.h>

#include "internal/platform/concurrency.h"
#include "record.h"
#include "zerobus/common.h"

/* Blocking wrapper around zb_queue_t. See queue.h for queue semantics.
 * Deadlines apply only to semaphore waits. */
typedef struct zb_landing_zone zb_landing_zone_t;

/* Capacity must be a power of two, at least 4, and less than
 * ZB_SEM_VALUE_MAX. */
zerobus_status_t zb_landing_zone_new(size_t capacity,
                                     zb_landing_zone_t **out_zone);

/* Takes ownership of the record on success. */
zerobus_status_t zb_landing_zone_push(zb_landing_zone_t *zone,
                                      zb_record_t *record,
                                      zb_deadline_t deadline,
                                      zerobus_offset_t *out_offset);

/* Returns a borrowed record through out_record, kept alive until pop or zone
 * free. After pop, the new owner controls its lifetime. */
zerobus_status_t zb_landing_zone_observe(zb_landing_zone_t *zone,
                                         zb_deadline_t deadline,
                                         zb_record_t **out_record,
                                         zerobus_offset_t *out_offset);

/* Returns ownership of the record and its JSON allocation to the caller. */
zb_record_t *zb_landing_zone_pop(zb_landing_zone_t *zone,
                                 zerobus_offset_t *out_offset);

/* Makes observed records available to observe again, keeping their offsets.
 * May run concurrently with push. Must be serialized with observe (including
 * waiting calls), pop and other reset_observe calls. */
zerobus_status_t zb_landing_zone_reset_observe(zb_landing_zone_t *zone);

/* Rejects pushes with FAILED_PRECONDITION and wakes waiting producers.
 * Idempotent. Admitted pushes may finish later. Observers are unaffected. */
zerobus_status_t zb_landing_zone_close(zb_landing_zone_t *zone);

/* Frees remaining records, their JSON allocations, and the zone. */
void zb_landing_zone_free(zb_landing_zone_t *zone);

#endif /* ZB_LANDING_ZONE_H */
