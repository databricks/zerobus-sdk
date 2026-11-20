// Implementation of StreamBuilder (declared in zerobus/stream_builder.hpp).
//
// Setters only store values. build() validates, then opens the stream through
// Sdk::open_stream, the same native create path as Sdk::create_stream, with the
// same StreamOptions conversion (detail::to_c). Public API documentation lives
// on the header.
#include "zerobus/stream_builder.hpp"

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
  if (!table_name_.has_value()) {
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

}  // namespace zerobus
