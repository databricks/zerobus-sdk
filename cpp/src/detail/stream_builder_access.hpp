#ifndef ZEROBUS_DETAIL_STREAM_BUILDER_ACCESS_HPP
#define ZEROBUS_DETAIL_STREAM_BUILDER_ACCESS_HPP

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
};

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_STREAM_BUILDER_ACCESS_HPP
