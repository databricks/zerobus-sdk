/*
 * Zerobus Pure C SDK — public API umbrella header.
 *
 * Includes the whole public surface. Per-area headers can be included directly:
 * common.h, error.h, sdk.h, stream.h.
 *
 * The SDK is under early development: the functions validate inputs and honor
 * the documented status/ownership rules, but the networking core is not
 * implemented yet.
 *
 * Logging: set ZEROBUS_LOG_LEVEL before launching the application to one of
 * off, error, warn, info, debug, trace (lowercase). Each level includes more
 * severe messages. Unset, empty, and off are silent in every build type.
 * An invalid nonempty value emits one warning, then disables logging.
 * Diagnostics go only to stderr, statuses and error objects are unaffected.
 *
 * Thread-safety: this SDK, including logging, currently requires a single
 * caller thread. Concurrent initialization and calls are not supported yet.
 */
#ifndef ZEROBUS_H
#define ZEROBUS_H

#include "zerobus/common.h" // IWYU pragma: export
#include "zerobus/error.h"  // IWYU pragma: export
#include "zerobus/sdk.h"    // IWYU pragma: export
#include "zerobus/stream.h" // IWYU pragma: export

#endif /* ZEROBUS_H */
