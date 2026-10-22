#ifndef ZB_SDK_H
#define ZB_SDK_H

#include "internal/network/tls.h"
#include "zerobus/common.h"

/* Caller must hold a live, non-NULL SDK reference; release with sdk_free. */
void zb_sdk_ref(zerobus_sdk_t *sdk);

/* Connection settings shared by the SDK's streams, borrowed from the SDK. */
typedef struct {
    const char *host;
    uint16_t port;
    bool plaintext;
    const zb_tls_ctx_t *tls_context; /* NULL for plaintext. */
    const char *uc_endpoint;
    const char *workspace_id; /* The first label of the Zerobus host. */
} zb_sdk_endpoint_t;

void zb_sdk_endpoint(const zerobus_sdk_t *sdk, zb_sdk_endpoint_t *out);

#endif /* ZB_SDK_H */
