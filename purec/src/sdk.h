#ifndef ZB_SDK_H
#define ZB_SDK_H

#include <stdatomic.h>

#include "internal/network/tls.h"
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
    /* Trusts the system's roots for an https uc_endpoint; NULL for http. */
    zb_tls_ctx_t *uc_tls_context;
    /* The Zerobus endpoint's parts: plaintext for http, with the scheme's
     * default port when none is given. */
    char *host;
    uint16_t port;
    bool plaintext;
    /* Trusts the system's roots for an https endpoint; NULL for http. */
    zb_tls_ctx_t *tls_context;
    /* "zerobus-sdk-c/<version>", then a space and the application name if
     * one is set. */
    char *user_agent;
};

/* Caller must hold a live, non-NULL SDK reference; release with sdk_free. */
void zb_sdk_ref(zerobus_sdk_t *sdk);

#endif /* ZB_SDK_H */
