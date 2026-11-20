#ifndef ZEROBUS_MESSAGE_ID_HPP
#define ZEROBUS_MESSAGE_ID_HPP

#include <cstddef>
#include <cstdint>
#include <functional>

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

#endif  // ZEROBUS_MESSAGE_ID_HPP
