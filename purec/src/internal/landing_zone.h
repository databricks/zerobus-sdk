#ifndef ZB_LANDING_ZONE_H
#define ZB_LANDING_ZONE_H

#include <stddef.h>

#include "ingest_request.h"
#include "internal/platform/concurrency.h"
#include "zerobus/common.h"

/* Blocking wrapper around zb_queue_t. See queue.h for queue semantics.
 * Deadlines apply only to semaphore waits. */
typedef struct zb_landing_zone zb_landing_zone_t;

/* Capacity must be a power of two, at least 4, and strictly less than
 * ZB_SEM_VALUE_MAX. */
zerobus_status_t zb_landing_zone_new(size_t capacity,
                                     zb_landing_zone_t **out_zone);

/* Takes ownership of the request on success. */
zerobus_status_t zb_landing_zone_push(zb_landing_zone_t *zone,
                                      zb_ingest_request_t *request,
                                      zb_deadline_t deadline,
                                      zerobus_offset_t *out_offset);

/* Returns a borrowed request through out_request, kept alive until pop or zone
 * free. After pop, the new owner controls its lifetime.
 *
 * Once the zone is closed, observe never waits: it returns a remaining
 * request, or RESOURCE_EXHAUSTED if none is left. */
zerobus_status_t zb_landing_zone_observe(zb_landing_zone_t *zone,
                                         zb_deadline_t deadline,
                                         zb_ingest_request_t **out_request,
                                         zerobus_offset_t *out_offset);

/* Returns ownership of the request and its payload to the caller. */
zb_ingest_request_t *zb_landing_zone_pop(zb_landing_zone_t *zone,
                                         zerobus_offset_t *out_offset);

/* Makes observed requests available to observe again, keeping their offsets.
 * May run concurrently with push. Must be serialized with observe (including
 * waiting calls), pop and other reset_observe calls. Breaking this can make a
 * later observe spin forever, because a concurrent push can wake a waiting
 * observe mid-rewind. */
zerobus_status_t zb_landing_zone_reset_observe(zb_landing_zone_t *zone);

/* Rejects new and waiting pushes with FAILED_PRECONDITION, waits for admitted
 * pushes to land, then wakes waiting observers. observe and pop are still
 * allowed. Idempotent, also when called concurrently. */
zerobus_status_t zb_landing_zone_close(zb_landing_zone_t *zone);

/* Offset of the most recent push, or -1 if nothing was pushed. May include a
 * push that has taken its offset but not returned yet. */
zerobus_offset_t zb_landing_zone_last_offset(const zb_landing_zone_t *zone);

/* Frees remaining requests, their payloads, and the zone. */
void zb_landing_zone_free(zb_landing_zone_t *zone);

#endif /* ZB_LANDING_ZONE_H */
