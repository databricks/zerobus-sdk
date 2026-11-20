#ifndef ZEROBUS_STREAM_BUILDER_HPP
#define ZEROBUS_STREAM_BUILDER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "zerobus/ack_callback.hpp"
#include "zerobus/config.hpp"
#include "zerobus/headers_provider.hpp"
#include "zerobus/stream.hpp"

namespace zerobus {

struct CZerobusSdk;  // opaque FFI handle (defined in zerobus.h)
class Sdk;

namespace detail {
struct StreamBuilderAccess;
}

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
/// `Sdk` it came from, which must outlive every `build()` call. It is not safe
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
  /// 1. See `StreamOptions` for this and the settings below.
  StreamBuilder& max_inflight_requests(std::size_t n);
  StreamBuilder& recovery(bool enabled);
  StreamBuilder& recovery_timeout_ms(std::uint64_t ms);
  StreamBuilder& recovery_backoff_ms(std::uint64_t ms);
  StreamBuilder& recovery_retries(std::uint32_t n);
  StreamBuilder& server_lack_of_ack_timeout_ms(std::uint64_t ms);
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

 private:
  friend class Sdk;
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

}  // namespace zerobus

#endif  // ZEROBUS_STREAM_BUILDER_HPP
