#ifndef ZEROBUS_DETAIL_STREAM_BUILDER_ACCESS_HPP
#define ZEROBUS_DETAIL_STREAM_BUILDER_ACCESS_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "detail/config_convert.hpp"
#include "zerobus/stream_builder.hpp"

namespace zerobus {
namespace detail {

/// Read access to a builder's collected settings, for tests: the C options a
/// `build()` would pass to the FFI, and the options before conversion.
struct StreamBuilderAccess {
  static const StreamOptions& options(const StreamBuilder& builder) {
    return builder.options_;
  }
  static CStreamConfigurationOptions c_options(const StreamBuilder& builder) {
    return to_c(builder.options_);
  }
  static const std::optional<std::string>& table_name(
      const StreamBuilder& builder) {
    return builder.table_name_;
  }
  static const std::vector<std::uint8_t>& descriptor(
      const StreamBuilder& builder) {
    return builder.descriptor_proto_;
  }

  // Multiplexed builder: the settings snapshot it holds, and its own state.
  static const StreamBuilder& base(const MultiplexedStreamBuilder& builder) {
    return builder.base_;
  }
  static std::size_t stream_count(const MultiplexedStreamBuilder& builder) {
    return builder.stream_count_;
  }
  static const std::shared_ptr<MultiplexedAckCallback>& ack_callback(
      const MultiplexedStreamBuilder& builder) {
    return builder.ack_callback_;
  }
};

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_STREAM_BUILDER_ACCESS_HPP
