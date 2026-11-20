#ifndef ZEROBUS_MULTIPLEXED_ACK_CALLBACK_HPP
#define ZEROBUS_MULTIPLEXED_ACK_CALLBACK_HPP

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace zerobus {

/// Identifies a record or batch ingested on a `MultiplexedStream` (Beta).
///
/// Returned by the mux ingest calls and passed to `wait_for_message_id()` and
/// to `MultiplexedAckCallback`. The raw value can be negative, so do not use
/// its sign as an error marker, and only use an ID with the stream that
/// returned it. IDs compare for equality and hash, but have no order: IDs from
/// different sub-streams are not ordered relative to each other.
class MessageId {
 public:
  /// Wrap a raw ID, for example one you stored earlier with `raw()`.
  constexpr explicit MessageId(std::int64_t raw) noexcept : raw_(raw) {}

  /// The raw 64-bit value.
  constexpr std::int64_t raw() const noexcept { return raw_; }

  friend constexpr bool operator==(MessageId a, MessageId b) noexcept {
    return a.raw_ == b.raw_;
  }
  friend constexpr bool operator!=(MessageId a, MessageId b) noexcept {
    return a.raw_ != b.raw_;
  }

 private:
  std::int64_t raw_;
};

/// Async ack/error notifications for a `MultiplexedStream` (Beta). Register it
/// with `MultiplexedStreamBuilder::ack_callback()`.
///
/// Separate from `AckCallback`, which receives ordered offsets of one stream:
/// - `on_ack` fires once per ingest call (one call for a whole batch) with its
///   message ID. Calls are in order within each sub-stream, but each
///   sub-stream delivers on its own background task, so calls from different
///   sub-streams can run concurrently and in no global order. Synchronize any
///   shared state.
/// - `on_error` fires for each pending record or batch of a sub-stream that
///   fails; the error also surfaces from `flush()`, `close()` and the waits.
///
/// Callbacks run on the SDK's background threads. Keep them short: a slow or
/// blocking callback holds that thread and can delay other stream work. They
/// must not call `MultiplexedStream` methods, and must not block on a thread
/// that is inside one.
///
/// Both methods are `noexcept`: unwinding across the C FFI boundary is UB, so
/// an escaping exception calls `std::terminate`.
///
/// Lifetime: the SDK keeps its own reference to the callback until the last
/// sub-stream and background task releases it, which can be after the
/// `MultiplexedStream` is destroyed (or after a failed `build()`). It is never
/// called on a destroyed object, but whatever it captures must stay valid
/// until then.
class MultiplexedAckCallback {
 public:
  virtual ~MultiplexedAckCallback() = default;

  /// Called when the record or batch behind @p id is acknowledged.
  virtual void on_ack(MessageId id) noexcept = 0;

  /// Called when the record or batch behind @p id failed.
  ///
  /// @param error_message Human-readable error text from the core.
  virtual void on_error(MessageId id,
                        const std::string& error_message) noexcept = 0;

  /// Adapt lambdas into a `MultiplexedAckCallback`. The error handler is
  /// optional. Both handlers must not throw.
  static std::shared_ptr<MultiplexedAckCallback> from(
      std::function<void(MessageId)> on_ack,
      std::function<void(MessageId, const std::string&)> on_error = nullptr);
};

}  // namespace zerobus

namespace std {

/// Hash a `MessageId` by its raw value, for unordered containers.
template <>
struct hash<zerobus::MessageId> {
  std::size_t operator()(zerobus::MessageId id) const noexcept {
    return std::hash<std::int64_t>{}(id.raw());
  }
};

}  // namespace std

#endif  // ZEROBUS_MULTIPLEXED_ACK_CALLBACK_HPP
