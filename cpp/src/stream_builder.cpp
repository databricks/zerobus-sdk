// Implementation of StreamBuilder (declared in zerobus/stream_builder.hpp).
//
// Setters only store values. build() validates, then opens the stream through
// Sdk::open_stream, the same native create path as Sdk::create_stream, with the
// same StreamOptions conversion (detail::to_c). Public API documentation lives
// on the header.
#include "zerobus/stream_builder.hpp"

#include <string>
#include <utility>

#include "zerobus/error.hpp"
#include "zerobus/sdk.hpp"

namespace zerobus {

StreamBuilder& StreamBuilder::table(const std::string& table_name) {
  table_name_ = table_name;
  return *this;
}

StreamBuilder& StreamBuilder::oauth(const std::string& client_id,
                                    const std::string& client_secret) {
  auth_ = Auth::OAuth;
  client_id_ = client_id;
  client_secret_ = client_secret;
  headers_provider_.reset();
  return *this;
}

StreamBuilder& StreamBuilder::headers_provider(
    std::shared_ptr<HeadersProvider> provider) {
  auth_ = Auth::HeadersProvider;
  headers_provider_ = std::move(provider);
  client_id_.clear();
  client_secret_.clear();
  return *this;
}

StreamBuilder& StreamBuilder::json() {
  format_set_ = true;
  options_.record_type = RecordType::Json;
  descriptor_proto_.clear();
  return *this;
}

StreamBuilder& StreamBuilder::compiled_proto(
    std::vector<std::uint8_t> descriptor_proto) {
  format_set_ = true;
  options_.record_type = RecordType::Proto;
  descriptor_proto_ = std::move(descriptor_proto);
  return *this;
}

StreamBuilder& StreamBuilder::max_inflight_requests(std::size_t n) {
  options_.max_inflight_requests = n;
  return *this;
}

StreamBuilder& StreamBuilder::recovery(bool enabled) {
  options_.recovery = enabled;
  return *this;
}

StreamBuilder& StreamBuilder::recovery_timeout_ms(std::uint64_t ms) {
  options_.recovery_timeout_ms = ms;
  return *this;
}

StreamBuilder& StreamBuilder::recovery_backoff_ms(std::uint64_t ms) {
  options_.recovery_backoff_ms = ms;
  return *this;
}

StreamBuilder& StreamBuilder::recovery_retries(std::uint32_t n) {
  options_.recovery_retries = n;
  return *this;
}

StreamBuilder& StreamBuilder::server_lack_of_ack_timeout_ms(std::uint64_t ms) {
  options_.server_lack_of_ack_timeout_ms = ms;
  return *this;
}

StreamBuilder& StreamBuilder::flush_timeout_ms(std::uint64_t ms) {
  options_.flush_timeout_ms = ms;
  return *this;
}

StreamBuilder& StreamBuilder::stream_paused_max_wait_time_ms(
    std::optional<std::uint64_t> ms) {
  options_.stream_paused_max_wait_time_ms = ms;
  return *this;
}

StreamBuilder& StreamBuilder::callback_wait_policy(CallbackWaitPolicy policy) {
  options_.callback_wait_policy = std::move(policy);
  return *this;
}

StreamBuilder& StreamBuilder::ack_callback(
    std::shared_ptr<AckCallback> callback) {
  options_.ack_callback = std::move(callback);
  return *this;
}

void StreamBuilder::validate() const {
  if (!table_name_.has_value() || table_name_->empty()) {
    throw ZerobusException("table name is required: call .table()", false);
  }
  if (auth_ == Auth::None) {
    throw ZerobusException(
        "authentication is required: call .oauth() or .headers_provider()",
        false);
  }
  if (auth_ == Auth::HeadersProvider && headers_provider_ == nullptr) {
    throw ZerobusException("headers_provider must not be null", false);
  }
  if (!format_set_) {
    throw ZerobusException(
        "record format is required: call .json() or .compiled_proto()", false);
  }
  if (options_.record_type == RecordType::Proto && descriptor_proto_.empty()) {
    throw ZerobusException(
        "compiled_proto() needs a non-empty serialized DescriptorProto", false);
  }
  // The core would accept 0 and build a stream whose first ingest waits
  // forever, so reject it here.
  if (options_.max_inflight_requests == 0) {
    throw ZerobusException("max_inflight_requests must be at least 1", false);
  }
}

Stream StreamBuilder::build() const {
  validate();
  TableProperties table;
  table.table_name = *table_name_;
  table.descriptor_proto = descriptor_proto_;
  if (auth_ == Auth::OAuth) {
    return Sdk::open_stream(sdk_, table, client_id_, client_secret_, options_);
  }
  return Sdk::open_stream(sdk_, table, headers_provider_, options_);
}

MultiplexedStreamBuilder StreamBuilder::multiplexed(
    std::size_t stream_count) const {
  return MultiplexedStreamBuilder(*this, stream_count);
}

MultiplexedStreamBuilder& MultiplexedStreamBuilder::ack_callback(
    std::shared_ptr<MultiplexedAckCallback> callback) {
  ack_callback_ = std::move(callback);
  return *this;
}

// Same checks and wording as the Rust core's multiplexed validate(), with the
// C++ setter names. The order differs in one case: StreamBuilder::validate()
// runs first, so max_inflight_requests 0 reports "at least 1" before the
// core's "at least stream_count".
void MultiplexedStreamBuilder::validate() const {
  base_.validate();
  if (stream_count_ < 1 || stream_count_ > 64) {
    throw ZerobusException("multiplexed stream_count must be between 1 and 64",
                           false);
  }
  const std::size_t max_inflight = base_.options_.max_inflight_requests;
  if (max_inflight < stream_count_) {
    throw ZerobusException(
        "max_inflight_requests (" + std::to_string(max_inflight) +
            ") must be at least stream_count (" +
            std::to_string(stream_count_) + ") for multiplexed streams",
        false);
  }
  if (base_.options_.ack_callback != nullptr) {
    throw ZerobusException(
        "ack_callback is only valid for ordinary streams; use "
        "multiplexed(n).ack_callback(...)",
        false);
  }
}

MultiplexedStream MultiplexedStreamBuilder::build() const {
  validate();
  TableProperties table;
  table.table_name = *base_.table_name_;
  table.descriptor_proto = base_.descriptor_proto_;
  if (base_.auth_ == StreamBuilder::Auth::OAuth) {
    return Sdk::open_multiplexed_stream(base_.sdk_, table, base_.client_id_,
                                        base_.client_secret_, stream_count_,
                                        base_.options_, ack_callback_);
  }
  return Sdk::open_multiplexed_stream(base_.sdk_, table,
                                      base_.headers_provider_, stream_count_,
                                      base_.options_, ack_callback_);
}

}  // namespace zerobus
