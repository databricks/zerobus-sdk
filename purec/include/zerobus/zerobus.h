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
 * Diagnostics go to stderr unless ZEROBUS_LOG_FILE names a file to append to.
 * An unset or empty file setting uses stderr. If opening the file fails, one
 * warning is written to stderr and subsequent diagnostics use stderr.
 * No file is opened when logging is disabled. Each message is flushed.
 *
 * Both settings are read on the first logging attempt (even a suppressed one)
 * and remain fixed for the run. An opened file stays open until process exit.
 * Statuses and error objects are unaffected by logging.
 *
 * Thread-safety: logging initialization and writes are synchronized. Other SDK
 * operations currently require a single caller thread; concurrent calls are
 * not supported yet.
 */
#ifndef ZEROBUS_H
#define ZEROBUS_H

#include "zerobus/common.h" // IWYU pragma: export
#include "zerobus/error.h"  // IWYU pragma: export
#include "zerobus/sdk.h"    // IWYU pragma: export
#include "zerobus/stream.h" // IWYU pragma: export

#endif /* ZEROBUS_H */
