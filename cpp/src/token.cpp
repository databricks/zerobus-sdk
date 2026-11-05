// Implementation of get_token (declared in zerobus/token.hpp).
#include "zerobus/token.hpp"

#include "detail/ffi_util.hpp"
#include "detail/token_convert.hpp"

namespace zerobus {

namespace {

// Frees the FFI token on every exit, including when copying it out throws.
struct TokenGuard {
  CZerobusToken token{};
  ~TokenGuard() { zerobus_free_token(&token); }
};

}  // namespace

OAuthToken get_token(const std::string& workspace_url,
                     const std::string& table_name,
                     const std::string& client_id,
                     const std::string& client_secret,
                     const std::string& workspace_id) {
  detail::ResultGuard result;
  TokenGuard guard;
  zerobus_get_token(detail::checked_c_str(workspace_url, "workspace_url"),
                    detail::checked_c_str(table_name, "table_name"),
                    detail::checked_c_str(client_id, "client_id"),
                    detail::checked_c_str(client_secret, "client_secret"),
                    detail::checked_c_str(workspace_id, "workspace_id"),
                    &guard.token, result.ptr());
  result.throw_if_error();
  return detail::to_oauth_token(guard.token);
}

}  // namespace zerobus
