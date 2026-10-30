#ifndef ZB_SDK_H
#define ZB_SDK_H

#include <stdatomic.h>

#include "zerobus/common.h"

struct zerobus_sdk {
    atomic_size_t ref_cnt;
    /* Independent copies of the validated configuration. */
    char *endpoint;
    char *uc_endpoint;
    char *workspace_id; /* The first label of the endpoint's host. */
    char *application_name;
    uint64_t token_refresh_buffer_ms;
    bool connection_per_stream;
};

/* Caller must hold a live, non-NULL SDK reference; release with sdk_free. */
void zb_sdk_ref(zerobus_sdk_t *sdk);

#endif /* ZB_SDK_H */
