/* An in-process Zerobus service for tests: EphemeralStream over plaintext
 * HTTP/2 and the OAuth token endpoint over HTTP/1.1, both on loopback and
 * served by one thread. */
#ifndef ZB_TEST_FAKE_ZEROBUS_H
#define ZB_TEST_FAKE_ZEROBUS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "zerobus/common.h"

typedef struct fake_zerobus fake_zerobus_t;

typedef struct {
    /* Nonzero: end every call at creation with this gRPC status. */
    uint32_t refuse_status;
    /* End the call with UNAVAILABLE once it received this many records,
     * for the first fail_calls calls. 0 never. */
    unsigned fail_after;
    unsigned fail_calls;
    /* Acknowledge only every this many records, and at half-close. 0 is 1. */
    unsigned ack_every;
    /* The authorization header every call must carry, else the call ends
     * UNAUTHENTICATED. NULL accepts any. */
    const char *authorization;
} fake_zerobus_options_t;

/* NULL options mean all defaults. */
zerobus_status_t fake_zerobus_start(const fake_zerobus_options_t *options,
                                    fake_zerobus_t **out_server);

/* The ports of the EphemeralStream and the token endpoints. */
uint16_t fake_zerobus_port(const fake_zerobus_t *server);
uint16_t fake_zerobus_token_port(const fake_zerobus_t *server);

/* Records received, calls created, and tokens issued so far. Records
 * resent after a failure are counted again. */
uint64_t fake_zerobus_records(fake_zerobus_t *server);

/* FNV-1a over the bytes of every record received, in order, starting from
 * FAKE_ZEROBUS_DIGEST_SEED. fake_zerobus_digest_add extends a digest by len
 * bytes the same way. */
#define FAKE_ZEROBUS_DIGEST_SEED UINT64_C(14695981039346656037)
uint64_t fake_zerobus_digest(fake_zerobus_t *server);
uint64_t fake_zerobus_digest_add(uint64_t digest, const void *data, size_t len);
unsigned fake_zerobus_calls(fake_zerobus_t *server);
unsigned fake_zerobus_tokens(fake_zerobus_t *server);

/* Stops the thread, closes every connection, and frees. NULL-safe. */
void fake_zerobus_stop(fake_zerobus_t *server);

#endif /* ZB_TEST_FAKE_ZEROBUS_H */
