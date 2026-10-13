#ifndef ZBP_POOL_H
#define ZBP_POOL_H
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

/* Reserve exact payload bytes before allocation; zero means budget pressure. */
int zbp_payload_reserve(_Atomic uint64_t *used, size_t budget, size_t size,
                        uint64_t *reserved);
/* Release only after freeing the payload so live storage stays bounded. */
void zbp_payload_release(_Atomic uint64_t *used, size_t size);
#endif
