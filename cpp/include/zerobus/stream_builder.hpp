#ifndef ZEROBUS_STREAM_BUILDER_HPP
#define ZEROBUS_STREAM_BUILDER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zerobus/ack_callback.hpp"
#include "zerobus/config.hpp"
#include "zerobus/headers_provider.hpp"
#include "zerobus/multiplexed_ack_callback.hpp"
#include "zerobus/multiplexed_stream.hpp"
#include "zerobus/stream.hpp"

namespace zerobus {

struct CZerobusSdk;  // opaque FFI handle (defined in zerobus.h)
class Sdk;

namespace detail {
struct StreamBuilderAccess;
}

class MultiplexedStreamBuilder;

/// Fluent builder for a proto/JSON `Stream` (Beta). Obtain one from
/// `Sdk::stream_builder()`.
///
/// Set a table, one authentication method and one record format, then call
/// `build()`:
///
/// ```
/// zerobus::Stream stream = sdk.stream_builder()
///                              .table("main.analytics.events")
///                              .oauth(client_id, client_secret)
///                              .json()
///                              .build();
/// ```
///
/// Setters never throw; they only store values. `validate()` and `build()`
/// check them and throw a non-retryable `ZerobusException`. A setting you do
/// not set keeps the same default as a default-constructed `StreamOptions`, so
/// an unset builder creates the same stream as `Sdk::create_stream`. The last
/// authentication setter wins, and so does the last format setter.
///
/// The builder is copyable and holds its settings by value. It refers to the
/// native handle of the `Sdk` it came from, which must stay alive and must not
/// be destroyed or move-assigned over while the builder is used. It is not safe
/// for concurrent use; `build()` may be called more than once, and each call
/// opens a new stream.
class StreamBuilder {
 public:
  /// Fully-qualified table name: `catalog.schema.table` (required).
  StreamBuilder& table(const std::string& table_name);

  /// Authenticate with OAuth client credentials.
  StreamBuilder& oauth(const std::string& client_id,
                       const std::string& client_secret);

  /// Authenticate with a custom headers provider. A null provider is rejected
  /// by `validate()`/`build()`.
  StreamBuilder& headers_provider(std::shared_ptr<HeadersProvider> provider);

  /// Ingest JSON records.
  StreamBuilder& json();

  /// Ingest protobuf records described by @p descriptor_proto, a serialized
  /// `DescriptorProto` (for example from `ProtoSchema::descriptor_bytes()`).
  StreamBuilder& compiled_proto(std::vector<std::uint8_t> descriptor_proto);

  /// Maximum number of in-flight (unacknowledged) requests. Must be at least
  /// 1. Each setter below maps to the `StreamOptions` field of the same name.
  StreamBuilder& max_inflight_requests(std::size_t n);
  /// Whether automatic stream recovery is enabled.
  StreamBuilder& recovery(bool enabled);
  /// Total time budget for a recovery attempt.
  StreamBuilder& recovery_timeout_ms(std::uint64_t ms);
  /// Backoff between recovery retries.
  StreamBuilder& recovery_backoff_ms(std::uint64_t ms);
  /// Number of recovery retries before giving up (0 = no retries).
  StreamBuilder& recovery_retries(std::uint32_t n);
  /// How long to wait for a server ack before the stream is stalled.
  StreamBuilder& server_lack_of_ack_timeout_ms(std::uint64_t ms);
  /// Time budget for `flush()` and the flush inside `close()`.
  StreamBuilder& flush_timeout_ms(std::uint64_t ms);
  /// `nullopt` waits the full server-specified pause duration.
  StreamBuilder& stream_paused_max_wait_time_ms(
      std::optional<std::uint64_t> ms);

  /// How long `close()` waits for pending ack callbacks. See
  /// `CallbackWaitPolicy`.
  StreamBuilder& callback_wait_policy(CallbackWaitPolicy policy);

  /// Async ack/error callback (`nullptr` = none). See `AckCallback` for the
  /// contract.
  StreamBuilder& ack_callback(std::shared_ptr<AckCallback> callback);

  /// Check the settings without opening a stream. Throws a non-retryable
  /// `ZerobusException` naming the first problem. Passing does not guarantee
  /// that `build()` succeeds: the server and the FFI check more.
  void validate() const;

  /// Validate, then open the stream. Throws `ZerobusException` on failure.
  Stream build() const;

  /// Switch to a multiplexed stream (Beta) of @p stream_count sub-streams
  /// (1 to 64). Returns a builder holding a copy of the settings so far;
  /// later changes to this builder do not reach it. `max_inflight_requests` is
  /// a budget for the whole stream: each sub-stream gets
  /// `max_inflight_requests / stream_count`, so it must be at least
  /// `stream_count`. Set the mux ack callback on the returned builder;
  /// `ack_callback()` here is only valid for ordinary streams.
  MultiplexedStreamBuilder multiplexed(std::size_t stream_count) const;

 private:
  friend class Sdk;
  friend class MultiplexedStreamBuilder;
  friend struct detail::StreamBuilderAccess;

  explicit StreamBuilder(CZerobusSdk* sdk) : sdk_(sdk) {}

  enum class Auth { None, OAuth, HeadersProvider };

  CZerobusSdk* sdk_;
  std::optional<std::string> table_name_;
  Auth auth_ = Auth::None;
  std::string client_id_;
  std::string client_secret_;
  std::shared_ptr<HeadersProvider> headers_provider_;
  bool format_set_ = false;
  std::vector<std::uint8_t> descriptor_proto_;
  // record_type is set by json()/compiled_proto(); every other field keeps the
  // StreamOptions default until its setter runs.
  StreamOptions options_;
};

/// Builder for a `MultiplexedStream` (Beta). Returned by
/// `StreamBuilder::multiplexed()`.
///
/// Like `StreamBuilder`: setters never throw, `validate()` and `build()` throw
/// a non-retryable `ZerobusException`, it is copyable and not thread-safe, and
/// the `Sdk` must outlive every `build()` call. Each sub-stream opens with a
/// random delay of up to 1 second, so `build()` takes up to that much longer
/// than for a single stream.
class MultiplexedStreamBuilder {
 public:
  /// Async ack/error callback shared by every sub-stream, receiving message
  /// IDs (`nullptr` = none). See `MultiplexedAckCallback` for the contract.
  MultiplexedStreamBuilder& ack_callback(
      std::shared_ptr<MultiplexedAckCallback> callback);

  /// Check the settings without opening any sub-stream. Throws a
  /// non-retryable `ZerobusException` naming the first problem: those of
  /// `StreamBuilder::validate()`, then a `stream_count` outside 1 to 64, a
  /// `max_inflight_requests` below `stream_count`, and an ordinary
  /// `ack_callback`.
  void validate() const;

  /// Validate, then open every sub-stream. If one fails to open, the others
  /// are closed and this throws `ZerobusException`.
  MultiplexedStream build() const;

 private:
  friend class StreamBuilder;
  friend struct detail::StreamBuilderAccess;

  MultiplexedStreamBuilder(StreamBuilder base, std::size_t stream_count)
      : base_(std::move(base)), stream_count_(stream_count) {}

  StreamBuilder base_;
  std::size_t stream_count_;
  std::shared_ptr<MultiplexedAckCallback> ack_callback_;
};

}  // namespace zerobus

#endif  // ZEROBUS_STREAM_BUILDER_HPP
