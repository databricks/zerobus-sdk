#ifndef ZEROBUS_MULTIPLEXED_STREAM_HPP
#define ZEROBUS_MULTIPLEXED_STREAM_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "zerobus/multiplexed_ack_callback.hpp"
#include "zerobus/record.hpp"

namespace zerobus {

struct CZerobusMultiplexedStream;  // opaque FFI handle (defined in zerobus.h)
class Sdk;

/// A multiplexed stream (Beta): several sub-streams to one table behind one
/// handle, to raise throughput when a single stream is the bottleneck.
///
/// Created with `sdk.stream_builder()...multiplexed(n).build()`. Each ingest
/// call goes to the next sub-stream, round-robin, and returns a `MessageId`.
/// Records keep their order within a sub-stream, but not across sub-streams.
/// As with `Stream`, ingest in a loop and call `flush()` once; do not wait on
/// every ID.
///
/// If a sub-stream fails, the stream stops accepting records once a call
/// observes the failure (`is_closed()` becomes true). Recover what was not
/// acknowledged with `get_unacked_records()` and send it on a new stream.
///
/// Thread safety: every method may be called concurrently from any thread.
/// Moving or destroying the stream must not overlap any other call on it.
///
/// Move-only. The destructor closes the stream (best effort, errors are
/// swallowed) and frees it, so call `close()` at a controlled point instead:
/// it flushes, can block up to `flush_timeout_ms`, and reports errors.
class MultiplexedStream {
 public:
  ~MultiplexedStream();

  MultiplexedStream(MultiplexedStream&& other) noexcept;
  MultiplexedStream& operator=(MultiplexedStream&& other) noexcept;
  MultiplexedStream(const MultiplexedStream&) = delete;
  MultiplexedStream& operator=(const MultiplexedStream&) = delete;

  /// Ingest a protobuf-encoded record into the next sub-stream. Returns once
  /// the record is queued; waits for room (up to 30 seconds) when that
  /// sub-stream is at capacity.
  ///
  /// A failed call queued nothing, and the record is not returned by
  /// `get_unacked_records()`. If `is_closed()` is then true, send it with the
  /// unacknowledged records on a new stream; otherwise retry it only when the
  /// exception `is_retryable()`.
  ///
  /// @throws ZerobusException if the stream is closed or ingestion fails.
  MessageId ingest_proto_record(const std::uint8_t* data, std::size_t len);

  /// @overload
  MessageId ingest_proto_record(const std::vector<std::uint8_t>& data);

  /// Ingest a JSON record into the next sub-stream. Same contract as
  /// `ingest_proto_record()`.
  MessageId ingest_json_record(const std::string& json);

  /// Ingest a batch of protobuf records into one sub-stream. The returned ID
  /// covers the whole batch; `std::nullopt` for an empty batch (a no-op). To
  /// spread load, send at least `stream_count` batches rather than one large
  /// one.
  ///
  /// @throws ZerobusException if the stream is closed or ingestion fails; a
  ///         failed call queued none of the batch.
  std::optional<MessageId> ingest_proto_records(
      const std::vector<std::vector<std::uint8_t>>& records);

  /// Ingest a batch of JSON records into one sub-stream. Same contract as
  /// `ingest_proto_records()`.
  std::optional<MessageId> ingest_json_records(
      const std::vector<std::string>& records);

  /// Block until the record or batch behind @p id is acknowledged. A failure
  /// of another sub-stream does not fail this wait. Pass only IDs returned by
  /// this stream. Prefer one `flush()` after an ingest loop.
  ///
  /// @throws ZerobusException if the owning sub-stream fails or the wait
  ///         exceeds `flush_timeout_ms`.
  void wait_for_message_id(MessageId id);

  /// Block until every queued record on every sub-stream is acknowledged.
  ///
  /// @throws ZerobusException if the flush fails. After a sub-stream failure,
  ///         recover records with `get_unacked_records()`.
  void flush();

  /// Flush and close every sub-stream. Waits for in-progress calls first. May
  /// be called more than once. The stream stays valid for
  /// `get_unacked_records()` and `is_closed()` until it is destroyed.
  ///
  /// @throws ZerobusException if the flush or close fails; then call
  ///         `get_unacked_records()`.
  void close();

  /// Whether the stream is closed: by `close()`, or because a call observed a
  /// sub-stream failure. Also true while a close is in progress and for a
  /// moved-from stream.
  bool is_closed() const;

  /// Close the stream if it is still open and return every record that was
  /// not acknowledged, for sending on a new stream. Errors from the implicit
  /// close are ignored, so recoverable records are still returned.
  ///
  /// @throws ZerobusException if the records cannot be retrieved.
  std::vector<UnackedRecord> get_unacked_records();

 private:
  friend class Sdk;
  explicit MultiplexedStream(CZerobusMultiplexedStream* handle)
      : handle_(handle) {}

  // Not changed by any method other than move and destruction, which is what
  // makes concurrent calls safe: the native handle is thread-safe.
  CZerobusMultiplexedStream* handle_;
};

}  // namespace zerobus

#endif  // ZEROBUS_MULTIPLEXED_STREAM_HPP
