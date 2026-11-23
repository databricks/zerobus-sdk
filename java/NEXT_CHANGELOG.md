# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- Stream creation and recovery retries now back off exponentially with jitter
  instead of waiting a fixed `recoveryBackoffMs` between attempts (from the Rust core).
  The wait still starts at `recoveryBackoffMs` and is never shorter than it. It
  doubles after each consecutive failed attempt, up to 15 seconds (or
  `recoveryBackoffMs` if larger), and a random jitter of up to the same amount is
  added. With the defaults (2 s, 4 retries), a stream that keeps failing gives
  up after about 29–58 s of waiting instead of 8 s. Clients rejected at the same
  time, for example by the server's stream-creation rate limit, no longer all
  retry at the same moment. No API change; a backoff of 0 still retries
  immediately.

### Bug Fixes

- Fix `AckCallback` notifications being silently dropped after a callback throws. An exception from `onAck` or `onError` was left pending on the SDK thread that ran the callback, so every later callback on that thread, including callbacks of other streams, failed without reaching Java. The exception is now cleared and normally logged at ERROR level with its stack trace through the SDK's SLF4J logger (visible when an SLF4J binding is configured; if logging itself fails, the exception is dropped), and later callbacks are delivered. Callbacks also no longer leave the thrown exception and the `onError` message referenced by native code after they return.
- Fix the JVM aborting when the native library panics. A panic reaching the JNI boundary aborted the whole process. A panic during a native method call now surfaces as a `NonRetriableException`, and a panic while the library loads makes `System.load` fail with `UnsatisfiedLinkError`.
- Calling a blocking SDK or stream method, such as `flush()`, `close()`, `waitForOffset()`, or `isClosed()`, from an `AckCallback` now throws `IllegalStateException`. Previously it aborted the JVM. Hand such work to another thread instead.

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, proto generator docs, and API docs link to it
  instead of repeating cloud-specific URLs.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
