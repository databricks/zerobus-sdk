#ifndef ZB_SDK_H
#define ZB_SDK_H

#include "zerobus/common.h"

/* Caller must hold a live, non-NULL SDK reference; release with sdk_free. */
void zb_sdk_ref(zerobus_sdk_t *sdk);

#endif /* ZB_SDK_H */
