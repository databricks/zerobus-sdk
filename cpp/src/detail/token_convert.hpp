#ifndef ZEROBUS_DETAIL_TOKEN_CONVERT_HPP
#define ZEROBUS_DETAIL_TOKEN_CONVERT_HPP

#include <chrono>
#include <optional>

#include "zerobus.h"
#include "zerobus/token.hpp"

namespace zerobus {
namespace detail {

/// Copies a token out of the FFI struct, which the caller still owns.
inline OAuthToken to_oauth_token(const CZerobusToken& token) {
  OAuthToken out{token.access_token, std::nullopt};
  if (token.expires_at_unix_ms != 0) {
    out.expires_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(token.expires_at_unix_ms));
  }
  return out;
}

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_TOKEN_CONVERT_HPP
